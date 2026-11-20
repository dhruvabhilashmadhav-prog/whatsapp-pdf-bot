import os
import io
from PIL import Image as PILImage, ImageOps
import img2pdf

def format_file_size(size_in_bytes: int) -> str:
    if size_in_bytes < 1024:
        return f"{size_in_bytes} B"
    elif size_in_bytes < 1024 * 1024:
        return f"{size_in_bytes / 1024:.1f} KB"
    else:
        return f"{size_in_bytes / (1024 * 1024):.2f} MB"

def prepare_image_for_pdf(image_path: str) -> bytes:
    """Prepares an image for img2pdf, handling EXIF orientation, alpha channels, and unsupported formats."""
    with PILImage.open(image_path) as img:
        # Handle EXIF orientation if present
        try:
            img = ImageOps.exif_transpose(img)
        except Exception:
            pass

        # Convert palette or alpha images to RGB
        if img.mode in ("RGBA", "LA", "P", "CMYK"):
            # If RGBA, create white background
            if img.mode in ("RGBA", "LA"):
                background = PILImage.new("RGB", img.size, (255, 255, 255))
                if img.mode == "RGBA":
                    background.paste(img, mask=img.split()[3])
                else:
                    background.paste(img, mask=img.split()[1])
                img = background
            else:
                img = img.convert("RGB")
        elif img.mode not in ("RGB", "L", "1"):
            img = img.convert("RGB")

        # Save as high-quality JPEG in memory for img2pdf
        buffer = io.BytesIO()
        img.save(buffer, format="JPEG", quality=95)
        return buffer.getvalue()

def convert_single_image_to_pdf(image_path: str, output_pdf_path: str) -> dict:
    """Converts a single image to PDF using img2pdf and Pillow."""
    os.makedirs(os.path.dirname(output_pdf_path), exist_ok=True)
    img_bytes = prepare_image_for_pdf(image_path)
    pdf_bytes = img2pdf.convert(img_bytes)

    with open(output_pdf_path, "wb") as f:
        f.write(pdf_bytes)

    file_size_bytes = os.path.getsize(output_pdf_path)
    return {
        "page_count": 1,
        "file_size": format_file_size(file_size_bytes),
        "file_size_bytes": file_size_bytes
    }

def convert_multiple_images_to_pdf(image_paths: list[str], output_pdf_path: str) -> dict:
    """Merges multiple images into a single multi-page PDF using img2pdf and Pillow."""
    if not image_paths:
        raise ValueError("No images provided for PDF conversion")

    os.makedirs(os.path.dirname(output_pdf_path), exist_ok=True)
    img_bytes_list = [prepare_image_for_pdf(path) for path in image_paths]
    pdf_bytes = img2pdf.convert(img_bytes_list)

    with open(output_pdf_path, "wb") as f:
        f.write(pdf_bytes)

    file_size_bytes = os.path.getsize(output_pdf_path)
    return {
        "page_count": len(image_paths),
        "file_size": format_file_size(file_size_bytes),
        "file_size_bytes": file_size_bytes
    }
