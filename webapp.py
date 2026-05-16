import os
import shutil
import subprocess
import time
import re
import uuid
from flask import Flask, render_template_string, request, send_file, after_this_request
from PIL import Image

app = Flask(__name__)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Gallery Downloader</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-900 text-white min-h-screen flex items-center justify-center p-4">
    <div class="max-w-md w-full bg-gray-800 rounded-xl shadow-lg p-8 space-y-6">
        <div class="text-center">
            <h1 class="text-3xl font-bold text-purple-400">Gallery Downloader</h1>
            <p class="text-gray-400 mt-2 text-sm">Download full galleries or specific page ranges as a ZIP file.</p>
        </div>
        
        <form action="/download" method="POST" class="space-y-5">
            <div>
                <label class="block text-sm font-medium text-gray-300 mb-1">Gallery URL</label>
                <input type="text" name="url" required placeholder="https://e-hentai.org/g/..." 
                       class="w-full px-4 py-3 bg-gray-700 border border-gray-600 rounded-lg focus:ring-2 focus:ring-purple-500 focus:outline-none text-white transition">
            </div>
            <div>
                <label class="block text-sm font-medium text-gray-300 mb-1">Pages Range</label>
                <input type="text" name="range" required placeholder="e.g., 0 (All), 1-10, !8" value="0" 
                       class="w-full px-4 py-3 bg-gray-700 border border-gray-600 rounded-lg focus:ring-2 focus:ring-purple-500 focus:outline-none text-white transition">
            </div>
            <button type="submit" 
                    class="w-full py-3 px-4 bg-purple-600 hover:bg-purple-700 rounded-lg font-bold text-white shadow-lg transition transform hover:-translate-y-0.5">
                Download ZIP
            </button>
        </form>
    </div>
</body>
</html>
"""

def process_single_image(filepath):
    if os.path.getsize(filepath) == 0: return None
    ext = filepath.lower().split('.')[-1]
    if ext in ['webp', 'gif', 'bmp']:
        new_path = f"{os.path.splitext(filepath)[0]}.jpg"
        try:
            img = Image.open(filepath).convert("RGB")
            img.save(new_path, "JPEG", quality=95, subsampling=0)
            return new_path
        except:
            return None
    return filepath

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/download', methods=['POST'])
def download():
    url = request.form.get('url', '').strip()
    page_range = request.form.get('range', '0').strip()
    
    if not url:
        return "Error: URL is required", 400
        
    req_id = str(uuid.uuid4())[:8]
    temp_dir = f"downloads/web_req_{req_id}"
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        # Run gallery-dl
        cmd = ["gallery-dl", "-d", temp_dir]
        if page_range != "0":
            cmd.extend(["--range", page_range])
        cmd.append(url)
        
        subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Process and Sanitize exactly like the Telegram Bot
        image_paths = []
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                filepath = os.path.join(root, file)
                processed = process_single_image(filepath)
                if processed:
                    image_paths.append(processed)
                    
        if not image_paths:
            shutil.rmtree(temp_dir, ignore_errors=True)
            return "Error: No images found or download blocked by site.", 404
            
        for old_path in image_paths:
            dir_name = os.path.dirname(old_path)
            original_filename = os.path.basename(old_path)
            safe_filename = re.sub(r'[^a-zA-Z0-9_.-]', '', original_filename)
            if not safe_filename or safe_filename.startswith('.'):
                safe_filename = f"page_{int(time.time())}.jpg"
                
            new_path = os.path.join(dir_name, safe_filename)
            if old_path != new_path:
                shutil.move(old_path, new_path)
                
        # Zip the processed folder
        zip_filename = f"gallery_{req_id}"
        zip_path = shutil.make_archive(os.path.join("downloads", zip_filename), 'zip', temp_dir)
        
        # Clean up the raw folder
        shutil.rmtree(temp_dir, ignore_errors=True)
        
        # Send the Zip file to the user
        return send_file(zip_path, as_attachment=True, download_name=f"{zip_filename}.zip")
        
    except Exception as e:
        return f"An error occurred: {str(e)}", 500

if __name__ == '__main__':
    # Binds to the single public port Hugging Face allows
    app.run(host='0.0.0.0', port=7860)
    