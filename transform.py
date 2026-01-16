"""
Data transformation utilities
Converts API response to CSV format
"""

import hashlib
import json
import re
from datetime import datetime


def generate_slug(name: str, product_id: str) -> str:
    """Generate URL-friendly slug from product name"""
    slug = re.sub(r'[^\w\s-]', '', name.lower())
    slug = re.sub(r'[\s_]+', '-', slug)
    slug = re.sub(r'-+', '-', slug).strip('-')
    return f"{slug}-{product_id}"[:100]


def generate_md5(data: dict) -> str:
    """Generate MD5 hash for product data"""
    content = f"{data.get('id', '')}{data.get('name', '')}{data.get('special_price', '')}"
    return hashlib.md5(content.encode()).hexdigest()


def parse_category(category_str: str, tag: str) -> tuple:
    """Parse category - API returns numeric IDs, use generic categories"""
    # API returns category as "2/163" (numeric IDs) - no real category names
    # Use "Fashion" / "Products" as generic values since real names unavailable
    level1 = "Fashion"
    level2 = "Products" 
    level3 = ""
    
    # If category has non-numeric parts, use them
    if category_str:
        parts = category_str.split('/')
        for part in parts:
            if part and not part.isdigit():
                if not level1 or level1 == "Fashion":
                    level1 = part.title()
                elif not level2 or level2 == "Products":
                    level2 = part.title()
                else:
                    level3 = part.title()
                    break
    
    return level1, level2, level3


def format_image_urls(main_image: str, pictures: list) -> str:
    """Format image URLs as JSON array string"""
    all_images = []
    
    if isinstance(pictures, list) and pictures:
        all_images.extend([img for img in pictures if img])
    
    if not all_images and main_image:
        all_images.append(main_image)
    
    return json.dumps(all_images) if all_images else "[]"


def format_datetime(dt_str: str = None) -> str:
    """Format datetime string to DD/MM/YYYY HH:MM:SS"""
    if not dt_str:
        return datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    try:
        dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        return dt.strftime("%d/%m/%Y %H:%M:%S")
    except:
        return datetime.now().strftime("%d/%m/%Y %H:%M:%S")


def transform_product(product: dict) -> dict:
    """Transform API product data to CSV format"""
    product_id = product.get('id', 0)
    name = product.get('name', '')
    tag = product.get('tag', '')
    category = product.get('category', '')
    
    level1, level2, level3 = parse_category(category, tag)
    
    # Info API returns 'picture' array, list API returns 'thumbnail'
    main_image = product.get('thumbnail', '')
    pictures = product.get('picture', product.get('pictures', []))
    image_urls = format_image_urls(main_image, pictures)
    
    # Use long_desc or short_desc if available, fallback to name
    description = product.get('long_desc', '') or product.get('short_desc', '') or product.get('seo_description', '') or name
    
    return {
        "id": product_id,
        "slug": generate_slug(name, str(product_id)),
        "title": name,
        "description": description,
        "price": product.get('special_price', product.get('originalPrice', product.get('price', 0))),
        "category_level1": level1,
        "category_level2": level2,
        "category_level3": level3,
        "image_urls": image_urls,
        "main_image": main_image if main_image else (pictures[0] if pictures else ''),
        "created_at": format_datetime(product.get('created_at', '')),
        "updated_at": format_datetime(product.get('updated_at', '')),
        "md5": generate_md5(product),
        "jump": f"https://filovesk.click/product_details/{product_id}.html"
    }

