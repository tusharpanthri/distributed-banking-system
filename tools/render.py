#!/usr/bin/env python3
"""Render a recorded session into the README's animation and screenshots.

Input is the JSONL written by ``go run ./tools/transcript`` -- real frames off a
real WebSocket, not a mock-up. This script only decides typography and timing;
every word it draws came off the wire.

    go run ./tools/transcript -out docs/screenshots/transcript.jsonl
    python tools/render.py

Writes docs/screenshots/tour.gif plus a handful of stills.
"""

import json
import os
import subprocess
import sys
import tempfile

from PIL import Image, ImageDraw, ImageFont

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRANSCRIPT = os.path.join(ROOT, "docs", "screenshots", "transcript.jsonl")
OUTDIR = os.path.join(ROOT, "docs", "screenshots")

# Terminal geometry, in characters.
COLS = 104
ROWS = 32

FONT_SIZE = 15
LINE_H = 20
PAD = 14

BG = (13, 17, 23)
PANEL = (22, 27, 34)
BORDER = (48, 54, 61)

# The seven protocol levels, plus the colours the frontend uses for everything
# it draws locally. Kept in step with frontend/index.html by eye; they are the
# same palette.
COLOR = {
    "paxos": (86, 182, 194),
    "twopc": (197, 134, 192),
    "leader": (229, 192, 123),
    "net": (97, 139, 210),
    "error": (248, 81, 73),
    "client": (63, 185, 80),
    "info": (139, 148, 158),
}
DIM = (110, 118, 129)
TEXT = (201, 209, 217)
NARR = (214, 180, 140)
PROMPT_C = (88, 166, 255)

# The GIF covers only the failure arc. A README animation competes with the
# reader's patience, not with a documentary: ninety seconds of setup gets
# scrolled past, whereas twenty seconds of "kill the leader, watch it re-elect,
# kill another, watch it refuse" is the whole argument. The stills carry the
# rest.
GIF_FROM = "now break it"          # narration that opens the window
GIF_TO = "put tushar 999"           # command whose result closes it

# Downscale for the README. Full size is kept for the stills, which people
# actually zoom into.
GIF_SCALE = 0.72

# Stills worth keeping, keyed by a substring of the command that produces them.
STILLS = {
    "transfer tushar varun 25": "cross-shard-2pc.png",
    "datastore": "datastore-converged.png",
    "kill s2n0": "leader-reelection.png",
    "put tushar 999": "no-quorum-refuses.png",
    "partition s2n0 | s2n1 | s2n2": "partitioned-leaderless.png",
    "heal": "heal-recovers.png",
}


def load_font(size, bold=False):
    candidates = [
        r"C:\Windows\Fonts\consolab.ttf" if bold else r"C:\Windows\Fonts\consola.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf" if bold
        else "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
        "/System/Library/Fonts/Menlo.ttc",
    ]
    for path in candidates:
        if os.path.exists(path):
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()


FONT = load_font(FONT_SIZE)
FONT_B = load_font(FONT_SIZE, bold=True)


def wrap(text, width):
    """Word-wrap, matching what the frontend does to narration."""
    out, cur = [], ""
    for word in text.split(" "):
        if not cur:
            cur = word
        elif len(cur) + 1 + len(word) <= width:
            cur += " " + word
        else:
            out.append(cur)
            cur = word
    if cur:
        out.append(cur)
    return out


class Screen:
    """A scrolling buffer of (text, colour, bold) spans, one list per line."""

    def __init__(self):
        self.lines = []

    def add(self, spans):
        self.lines.append(spans)
        # Keep only what fits; the GIF is a window onto the tail of the session.
        if len(self.lines) > ROWS:
            self.lines = self.lines[-ROWS:]

    def blank(self):
        self.add([])


def char_w():
    img = Image.new("RGB", (10, 10))
    d = ImageDraw.Draw(img)
    return d.textlength("M", font=FONT)


CW = char_w()
WIDTH = int(PAD * 2 + CW * COLS)
HEIGHT = PAD * 2 + LINE_H * ROWS + 34


def render(screen):
    img = Image.new("RGB", (WIDTH, HEIGHT), BG)
    d = ImageDraw.Draw(img)

    # Title bar, so a still reads as an application rather than a code block.
    d.rectangle([0, 0, WIDTH, 30], fill=PANEL)
    d.line([(0, 30), (WIDTH, 30)], fill=BORDER)
    d.text((PAD, 8), "distributed transaction control plane", font=FONT_B, fill=TEXT)
    d.text((PAD + CW * 38, 8), "— paxos + 2pc", font=FONT, fill=DIM)
    d.text((WIDTH - PAD - CW * 9, 8), "connected", font=FONT, fill=COLOR["client"])

    y = 30 + PAD
    for spans in screen.lines:
        x = PAD
        for text, colour, bold in spans:
            d.text((x, y), text, font=FONT_B if bold else FONT, fill=colour)
            x += CW * len(text)
        y += LINE_H
    return img


def main():
    if not os.path.exists(TRANSCRIPT):
        sys.exit("missing %s -- run: go run ./tools/transcript" % TRANSCRIPT)

    records = [json.loads(l) for l in open(TRANSCRIPT, encoding="utf-8") if l.strip()]

    screen = Screen()
    frames = []   # (PIL image, duration ms), only within the GIF window
    stills = {}
    window = [False, False]  # [open, closed]

    def snap(ms):
        if window[0] and not window[1]:
            frames.append((render(screen), ms))

    for rec in records:
        kind = rec.get("type")
        msg = rec.get("msg", "")

        if kind == "narr" and GIF_FROM in msg:
            window[0] = True

        if kind == "narr":
            screen.blank()
            for l in wrap(msg, COLS - 4):
                screen.add([("  " + l, NARR, False)])
            screen.blank()
            snap(1500)

        elif kind == "cmd":
            screen.add([("> ", PROMPT_C, False), (msg, TEXT, False)])
            snap(700)

        elif kind == "log":
            colour = COLOR.get(rec.get("level", "info"), COLOR["info"])
            tag = rec.get("tag", "")
            spans = [("  ", TEXT, False)]
            spans.append((tag.ljust(8), DIM, False))
            spans.append((msg[: COLS - 12], colour, False))
            screen.add(spans)
            snap(190)

        elif kind == "result":
            if msg:
                tint = COLOR["info"]
                if msg.startswith("OK"):
                    tint = COLOR["client"]
                elif msg.startswith("ABORT"):
                    tint = COLOR["leader"]
                elif msg.startswith("ERROR"):
                    tint = COLOR["error"]
                screen.add([("  ", TEXT, False), (msg[: COLS - 4], tint, True)])
                snap(900)

            # Hold a still at the interesting moments.
            for needle, name in STILLS.items():
                if needle == pending_cmd[0]:
                    stills[name] = render(screen)

            if pending_cmd[0] == GIF_TO:
                # Linger on the refusal: it is the point of the whole clip.
                if window[0] and not window[1]:
                    frames.append((render(screen), 2600))
                window[1] = True

        if kind == "cmd":
            pending_cmd[0] = msg

    # A key that matches nothing produces no image and no complaint, which is
    # how a rename silently guts this output. Fail instead.
    unmatched = sorted(set(STILLS.values()) - set(stills))
    if unmatched:
        sys.exit("no frame matched these stills: %s -- check the command "
                 "strings in STILLS against the transcript"
                 % ", ".join(unmatched))
    if not window[0]:
        sys.exit("GIF_FROM %r never matched any narration" % GIF_FROM)
    if not window[1]:
        sys.exit("GIF_TO %r never matched any command" % GIF_TO)

    os.makedirs(OUTDIR, exist_ok=True)

    for name, img in stills.items():
        img.save(os.path.join(OUTDIR, name))
        print("wrote", name)

    if not frames:
        sys.exit("no frames captured -- check GIF_FROM / GIF_TO")
    write_gif(frames)


pending_cmd = [""]


def write_gif(frames):
    """Encode with ffmpeg when available -- its palette handling is far better
    than PIL's for this kind of flat-colour text -- and fall back to PIL."""
    out = os.path.join(OUTDIR, "tour.gif")

    with tempfile.TemporaryDirectory() as tmp:
        concat = []
        for i, (img, ms) in enumerate(frames):
            path = os.path.join(tmp, "f%05d.png" % i)
            if GIF_SCALE != 1.0:
                img = img.resize(
                    (int(img.width * GIF_SCALE), int(img.height * GIF_SCALE)),
                    Image.LANCZOS)
            img.save(path)
            concat.append("file '%s'\nduration %.3f\n" % (path.replace("\\", "/"), ms / 1000.0))
        # ffmpeg's concat demuxer ignores the final entry's duration unless the
        # last file is repeated.
        concat.append("file '%s'\n" % os.path.join(tmp, "f%05d.png" % (len(frames) - 1)).replace("\\", "/"))

        listfile = os.path.join(tmp, "frames.txt")
        with open(listfile, "w", encoding="utf-8") as f:
            f.write("".join(concat))

        palette = os.path.join(tmp, "palette.png")
        try:
            subprocess.run(
                ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", listfile,
                 "-vf", "palettegen=max_colors=64:stats_mode=diff", palette],
                check=True, capture_output=True)
            subprocess.run(
                ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", listfile,
                 "-i", palette, "-lavfi", "paletteuse=dither=none", "-loop", "0", out],
                check=True, capture_output=True)
            print("wrote tour.gif (%d frames, ffmpeg)" % len(frames))
            return
        except (FileNotFoundError, subprocess.CalledProcessError) as e:
            print("ffmpeg unavailable or failed (%s), falling back to PIL" % type(e).__name__)

    images = [f[0] for f in frames]
    images[0].save(out, save_all=True, append_images=images[1:],
                   duration=[f[1] for f in frames], loop=0, optimize=True)
    print("wrote tour.gif (%d frames, PIL)" % len(frames))


if __name__ == "__main__":
    main()
