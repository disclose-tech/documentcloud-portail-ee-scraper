"""PDF compression with Ghostscript, to fit DocumentCloud's upload limit.

Text is kept: only images are resampled, and never below 150 dpi.
"""

import os
import shutil
import subprocess
import time

# DocumentCloud refuses files of 501 MiB or more (see check_size in documentcloud.documents)
MAX_UPLOAD_SIZE = 500 * 1024 * 1024

GS_TIMEOUT = 30 * 60  # seconds, per step

GS_COMMON_ARGS = ["-sDEVICE=pdfwrite", "-dNOPAUSE", "-dQUIET", "-dBATCH", "-dSAFER"]

# No visible change: JPEG images are kept as is, other images are recompressed losslessly
GS_LOSSLESS_ARGS = [
    "-dDownsampleColorImages=false",
    "-dDownsampleGrayImages=false",
    "-dDownsampleMonoImages=false",
    "-dPassThroughJPEGImages=true",
    "-dPassThroughJPXImages=true",
    "-dAutoFilterColorImages=false",
    "-dAutoFilterGrayImages=false",
    "-dColorImageFilter=/FlateEncode",
    "-dGrayImageFilter=/FlateEncode",
]

# Steps tried in order until the file fits: (name, image resolution or None for lossless)
GS_STEPS = [
    ("lossless", None),
    ("300 dpi", 300),
    ("200 dpi", 200),
    ("150 dpi", 150),
]


def gs_step_args(dpi):
    """Ghostscript arguments of a step: lossless, or every image above `dpi` resampled to `dpi`.

    Black & white images are kept at 300 dpi at least (cheap, and needed for legibility).
    """

    if dpi is None:
        return GS_LOSSLESS_ARGS

    return [
        "-dDownsampleColorImages=true",
        "-dDownsampleGrayImages=true",
        "-dDownsampleMonoImages=true",
        f"-dColorImageResolution={dpi}",
        f"-dGrayImageResolution={dpi}",
        f"-dMonoImageResolution={max(dpi, 300)}",
        "-dColorImageDownsampleThreshold=1.0",
        "-dGrayImageDownsampleThreshold=1.0",
        "-dMonoImageDownsampleThreshold=1.0",
        "-dColorImageDownsampleType=/Bicubic",
        "-dGrayImageDownsampleType=/Bicubic",
    ]


def size_mb(size):
    return f"{size / 1024 / 1024:.1f} MB"


def ghostscript_available():
    return shutil.which("gs") is not None


def compress_pdf(path, max_size, logger):
    """Compresses the PDF at `path` until it is under `max_size` bytes.

    Each step starts from the original file, which is kept. Returns the path of the
    compressed copy (next to the original), or None if Ghostscript failed or no step
    got under `max_size`.
    """

    original_size = os.path.getsize(path)
    directory, filename = os.path.split(path)
    compressed_path = os.path.join(directory, "compressed_" + filename)

    for step, dpi in GS_STEPS:
        start = time.monotonic()
        try:
            subprocess.run(
                [
                    "gs",
                    *GS_COMMON_ARGS,
                    *gs_step_args(dpi),
                    f"-sOutputFile={compressed_path}",
                    path,
                ],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                timeout=GS_TIMEOUT,
            )
        except (subprocess.SubprocessError, OSError) as e:
            stderr = (getattr(e, "stderr", None) or b"").decode(errors="replace").strip()
            logger.warning(
                f"Ghostscript failed on {path} ({step}): {type(e).__name__} {stderr[-500:]}"
            )
            break

        compressed_size = os.path.getsize(compressed_path)
        logger.info(
            f"Compressed {path} ({step}): {size_mb(original_size)} -> "
            f"{size_mb(compressed_size)} in {time.monotonic() - start:.0f}s"
        )

        if compressed_size <= max_size:
            return compressed_path

    if os.path.isfile(compressed_path):
        os.remove(compressed_path)

    return None
