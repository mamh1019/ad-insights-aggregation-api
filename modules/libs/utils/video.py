#!/usr/bin/env python
import subprocess
from pathlib import Path


def extract_first_frame(
    url: str,
    out_path: str | Path,
    width: int = 640,
    user_agent: str | None = None,
    *,
    timeout: int = 60,
) -> None:
    """
    Requires ffmpeg. Extract frame at 1s, scale to width (scale=width:-2).
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    vf = f"scale={width}:-2"
    cmd = ["/usr/local/bin/ffmpeg", "-hide_banner", "-loglevel", "error", "-y"]
    if user_agent:
        cmd += ["-user_agent", user_agent]
    cmd += [
        "-ss",
        "1",
        "-i",
        url,
        "-vf",
        vf,
        "-vframes",
        "1",
        "-q:v",
        "2",
        str(out_path),
    ]

    try:
        subprocess.run(cmd, check=True, timeout=timeout)
    except FileNotFoundError as e:
        raise RuntimeError("ffmpeg is not installed.") from e
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"ffmpeg execution failed (exit={e.returncode})") from e
    except subprocess.TimeoutExpired as e:
        raise RuntimeError("ffmpeg execution timed out.") from e
