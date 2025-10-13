#!/usr/bin/env python3
"""
Test the working file downloader with your known good parameters
"""

import logging
from file_downloader import FileDownloader

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_working_download():
    """Test with the exact parameters that worked in debug"""
    
    # Your working parameters from debug
    DOCUMENT_TYPE = "Plans"
    DOCUMENT_ID = "138646059"
    PROJECT_ID = "5871533"
    DISPLAY_NAME = "CS Cover Sheet"
    
    # You'll need to get a fresh token from your auth system
    # This is the same token you used in debug script
    ACCESS_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImVmMjQ4ZjQyZjc0YWUwZjk0OTIwYWY5YTlhMDEzMTdlZjJkMzVmZTEiLCJ0eXAiOiJKV1QifQ.eyJuYW1lIjoiTVVTU0lFIFRFV0VMREUiLCJmaXJzdG5hbWUiOiJNVVNTSUUiLCJyb2xlIjoyLCJ0YWtlb2ZmT3JnSWQiOiJmNGRiNWVkOS1iY2NiLTQ4YmItOGU4Zi1kYmY2OTI4YzZhZTgiLCJ0YWtlb2ZmT3JnUGF5bWVudFN0YXR1cyI6IlVucGFpZCIsInRha2VvZmZTdGF0dXMiOiJVbnBhaWQiLCJyb2xlVHlwZSI6IlN0YW5kYXJkIiwibW9kdWxlcyI6WyJQcm9qZWN0SW50ZWxsaWdlbmNlIl0sInAyaWQiOiI4MzgxNjAiLCJsYXN0bmFtZSI6IlRFV0VMREUiLCJmZWF0dXJlcyI6WyJzZWFyY2hUYWdzIiwibmV0d29ya1Byb21vdGlvbiIsInByb2plY3RMZWFkcyIsInByb2plY3REb2N1bWVudHNBY2Nlc3MiLCJleHBvcnQiLCJkb2N1bWVudFNlYXJjaCJdLCJzdGFnIjo1LCJvcmdfaWQiOiJmOGI1OTY4OC1hOWJmLTQ3MWQtOTMwYS1hZjBlMDE3ZmYwMmMiLCJyb2xlTmFtZSI6IkNvbXBhbnkgQWRtaW4iLCJwMmFjYyI6IjEwODIzMDE1IiwiaXNzIjoiaHR0cHM6Ly9zZWN1cmV0b2tlbi5nb29nbGUuY29tL2F1dGgtcHJvZC1zMGxyIiwiYXVkIjoiYXV0aC1wcm9kLXMwbHIiLCJhdXRoX3RpbWUiOjE3NTY0MzI0NzcsInVzZXJfaWQiOiJjMWNiMzk4My00NTdmLTQ2ZTMtOTljZi1hZjBlMDE3ZmYwZjUiLCJzdWIiOiJjMWNiMzk4My00NTdmLTQ2ZTMtOTljZi1hZjBlMDE3ZmYwZjUiLCJpYXQiOjE3NTY2Njk5MzYsImV4cCI6MTc1NjY3MzUzNiwiZW1haWwiOiJtdGV3ZWxkZUBoeWRybzJnZW90ZWNoLmNvbSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJmaXJlYmFzZSI6eyJpZGVudGl0aWVzIjp7ImVtYWlsIjpbIm10ZXdlbGRlQGh5ZHJvMmdlb3RlY2guY29tIl19LCJzaWduX2luX3Byb3ZpZGVyIjoicGFzc3dvcmQiLCJ0ZW5hbnQiOiJleHRlcm5hbC11c2Vycy16aXdwZCJ9fQ.k-tuOMp4n032zoVHHBITRyrwaDAa-LCbkJhslphCNTQnGdJD6e0qAd4x6peW9xvc0jpYb8C2EMXQYDpctWpUmAhAj1_PmmP320d0NXCclfMTkGf3NG24A-1wB2-YpMipotV-zzRhxVg0Y6GdoZ1OlNaG9LtuRDvqgY3G3FtpYL3VIxPdZFW5NIsJYdizELd4wpcfsNz0iGnpRBmZceVcWgaUswWZ2gIJfPMuYjE-dhTFtisVQXCSK7ufrcfCQXhyjuSd2PaXWXY6EeE412FyKh8sTDSBM90EqNObfGFZjNHE5t2Vbd0-Z5LrVLAN4a9LRO60aOUS-_eWzxS-jk3nLA"
    
    print("Testing Working File Downloader")
    print("=" * 50)
    
    # Initialize downloader
    downloader = FileDownloader(download_dir='downloads')
    
    # Test single download with exact working parameters
    result = downloader.test_single_download(
        document_type=DOCUMENT_TYPE,
        document_id=DOCUMENT_ID,
        project_id=PROJECT_ID,
        access_token=ACCESS_TOKEN,
        display_name=DISPLAY_NAME
    )
    
    if result['success']:
        print(f"\n🎉 SUCCESS! The downloader is now working!")
        print(f"Expected file size: ~4,748,908 bytes")
        print(f"Actual file size: {result['file_size']:,} bytes")
        
        if abs(result['file_size'] - 4748908) < 1000:
            print(f"✅ File size matches expected size!")
        else:
            print(f"⚠️ File size differs from expected")
            
    else:
        print(f"\n❌ Still having issues:")
        print(f"Error: {result['error']}")
        print(f"\nTroubleshooting steps:")
        print(f"1. Check if token is still valid (expires at 1756673536)")
        print(f"2. Verify network connectivity")
        print(f"3. Check if document is still accessible")
    
    return result

def test_with_fresh_token():
    """Test with a fresh token from your authentication system"""
    
    print("Testing with Fresh Token")
    print("=" * 30)
    print("To use this function:")
    print("1. Get a fresh token from your auth system")
    print("2. Replace the ACCESS_TOKEN variable")
    print("3. Run this function")
    
    # You would get this from: pm.auth.session_data['id_token']
    # fresh_token = "your_fresh_token_here"
    
    # For now, just show how to integrate with existing auth
    print("\nIntegration with existing code:")
    print("```python")
    print("from project_manager import ProjectManager")
    print("from file_downloader import FileDownloader")
    print("")
    print("pm = ProjectManager()")
    print("downloader = FileDownloader()")
    print("")
    print("if pm.auth.ensure_authenticated():")
    print("    result = downloader.test_single_download(")
    print("        document_type='Plans',")
    print("        document_id='138646059',")
    print("        project_id='5871533',")
    print("        access_token=pm.auth.session_data['id_token'],")
    print("        display_name='CS Cover Sheet'")
    print("    )")
    print("```")

if __name__ == "__main__":
    # Test with the token from debug (may be expired by now)
    result = test_working_download()
    
    print("\n" + "=" * 50)
    
    if not result['success']:
        print("Token might be expired. Try with fresh token:")
        test_with_fresh_token()
    else:
        print("🎉 All systems working! You can now:")
        print("1. Use this downloader in your main.py")
        print("2. Download multiple documents")
        print("3. Integrate with your existing workflow")