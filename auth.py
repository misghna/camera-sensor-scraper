import requests
import json
import logging
import pickle
import os
from configparser import ConfigParser
from datetime import datetime, timedelta


logger = logging.getLogger(__name__)

class ConstructConnectAuth:
    def __init__(self, credentials_file='credentials.ini'):
        self.base_url = "https://login.io.constructconnect.com"
        self.app_url = "https://app.constructconnect.com"
        self.session_file = 'session.pkl'
        self.credentials_file = credentials_file
        self.session_data = None
        self.api_key = None
        self.csrf_token = None
        
        # Create requests session for cookie management
        self.requests_session = requests.Session()
        
        # Load credentials
        self._load_credentials()
        
    def _load_credentials(self):
        """Load email and password from credentials.ini"""
        try:
            config = ConfigParser()
            config.read(self.credentials_file)
            self.email = config.get('credentials', 'email')
            self.password = config.get('credentials', 'password')
            logger.info("Credentials loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load credentials: {e}")
            raise
    
    def _save_session(self):
        """Save session data to file"""
        try:
            session_path = os.path.abspath(self.session_file)
            session_to_save = {
                'session_data': self.session_data,
                'csrf_token': self.csrf_token,
                'cookies': self.requests_session.cookies.get_dict()
            }
            with open(self.session_file, 'wb') as f:
                pickle.dump(session_to_save, f)
            logger.info(f"Session saved to: {session_path}")
            logger.info(f"Session valid until: {self.session_data['expires_at']}")
        except Exception as e:
            logger.error(f"Failed to save session: {e}")
    
    def _load_session(self):
        """Load session data from file"""
        try:
            if os.path.exists(self.session_file):
                session_path = os.path.abspath(self.session_file)
                logger.info(f"Found session file at: {session_path}")
                
                with open(self.session_file, 'rb') as f:
                    saved_session = pickle.load(f)
                
                # Handle both old and new session format
                if isinstance(saved_session, dict) and 'session_data' in saved_session:
                    self.session_data = saved_session['session_data']
                    self.csrf_token = saved_session.get('csrf_token')
                    # Restore cookies - clear first to avoid duplicates
                    self.requests_session.cookies.clear()
                    if 'cookies' in saved_session:
                        for name, value in saved_session['cookies'].items():
                            self.requests_session.cookies.set(name, value)
                else:
                    # Old format - just session data
                    self.session_data = saved_session
                
                # Check if token is still valid (12 hours)
                if self.session_data and 'expires_at' in self.session_data:
                    if datetime.now() < self.session_data['expires_at']:
                        time_left = self.session_data['expires_at'] - datetime.now()
                        logger.info(f"Valid session loaded - {time_left} remaining")
                        return True
                    else:
                        logger.info("Session expired, will need to re-authenticate")
                        return False
            else:
                logger.info(f"No session file found at: {os.path.abspath(self.session_file)}")
        except Exception as e:
            logger.error(f"Failed to load session: {e}")
        return False
    
    def clear_session(self):
        """Clear all session data and cookies"""
        logger.info("Clearing all session data")
        
        # Clear session data
        self.session_data = None
        self.csrf_token = None
        
        # Clear all cookies
        self.requests_session.cookies.clear()
        
        # Remove session file
        if os.path.exists(self.session_file):
            os.remove(self.session_file)
            logger.info(f"Removed session file: {self.session_file}")
    
    def get_api_key(self):
        """Step 1: Get API key from config endpoint"""
        logger.info("Step 1: Getting API key...")
        
        try:
            response = requests.get(f"{self.base_url}/api/config")
            response.raise_for_status()
            
            config_data = response.json()
            self.api_key = config_data.get('gcipApiKey')
            
            if self.api_key:
                logger.info(f"API key retrieved: {self.api_key[:20]}...")
                return True
            else:
                logger.error("API key not found in response")
                return False
                
        except Exception as e:
            logger.error(f"Failed to get API key: {e}")
            return False
    
    def login(self):
        """Step 2: Login and extract token"""
        logger.info("Step 2: Logging in...")
        
        if not self.api_key:
            logger.error("API key required for login")
            return False
            
        login_url = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={self.api_key}"
        
        login_data = {
            "returnSecureToken": True,
            "email": self.email,
            "password": self.password,
            "clientType": "CLIENT_TYPE_WEB",
            "tenantId": "external-users-ziwpd"
        }
        
        try:
            response = requests.post(login_url, json=login_data)
            response.raise_for_status()
            
            auth_response = response.json()
            
            # Extract important tokens and data
            # Extend session to 12 hours instead of API default (1 hour)
            session_duration = 1 * 55 * 60  # 55min
            self.session_data = {
                'id_token': auth_response.get('idToken'),
                'refresh_token': auth_response.get('refreshToken'),
                'local_id': auth_response.get('localId'),
                'email': auth_response.get('email'),
                'expires_in': session_duration,
                'expires_at': datetime.now() + timedelta(seconds=session_duration)
            }
            
            # Set up cookies for the app domain
            self._setup_cookies()
            
            logger.info(f"Login successful for: {self.session_data['email']}")
            logger.info(f"Token expires at: {self.session_data['expires_at']}")
            
            return True
            
        except Exception as e:
            logger.error(f"Login failed: {e}")
            return False
    
    def _setup_cookies(self):
        """Set up cookies needed for app.constructconnect.com"""
        try:
            # Clear existing cookies first to avoid duplicates
            self.requests_session.cookies.clear()
            
            # Set CCGIPAuth cookie with the access token
            ccgipauth_value = json.dumps({
                "accessToken": self.session_data['id_token'],
                "refreshToken": self.session_data['refresh_token']
            })
            
            # Set cookie without domain specification
            self.requests_session.cookies.set('CCGIPAuth', ccgipauth_value)
            
            logger.info("Cookies set up successfully")
            logger.info(f"Current cookies: {list(self.requests_session.cookies.keys())}")
            
        except Exception as e:
            logger.error(f"Failed to setup cookies: {e}")
    
    def get_csrf_token(self):
        """Step 2.5: Get CSRF token from app"""
        logger.info("Step 2.5: Getting CSRF token...")
        
        try:
            csrf_url = f"{self.app_url}/api/csrf"
            
            # Set common headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
                'Referer': 'https://app.constructconnect.com/results',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin'
            }
            
            response = self.requests_session.get(csrf_url, headers=headers)
            response.raise_for_status()
            
            csrf_data = response.json()
            self.csrf_token = csrf_data.get('csrf')
            
            if self.csrf_token:
                # Set the _csrf cookie that browser uses
                self.requests_session.cookies.set('_csrf', self.csrf_token)
                logger.info(f"CSRF token retrieved and cookie set: {self.csrf_token[:20]}...")
                return True
            else:
                logger.error("CSRF token not found in response")
                return False
                
        except Exception as e:
            logger.error(f"Failed to get CSRF token: {e}")
            return False
    
    def ensure_authenticated(self):
        """Ensure we have a valid session with CSRF token"""
        # Try to load existing session first
        if self._load_session() and self.csrf_token:
            # Validate CSRF token by making a simple test call
            test_result = self._test_csrf_token()
            if test_result:
                logger.info("Using cached session and CSRF token")
                return True
            else:
                logger.info("Cached CSRF token invalid, re-authenticating")
        
        # Clear everything before starting fresh
        logger.info("Starting fresh authentication")
        self.clear_session()
        
        # If no valid session, get API key and login
        if not self.get_api_key():
            return False
            
        if not self.login():
            return False
            
        # Get CSRF token
        if not self.get_csrf_token():
            return False
        
        # Save complete session
        self._save_session()
        return True
    
    def _test_csrf_token(self):
        """Test if current CSRF token is valid with a simple API call"""
        try:
            test_url = f"{self.app_url}/api/csrf"  # Re-test the CSRF endpoint
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
                'csrf-token': self.csrf_token
            }
            
            response = self.requests_session.get(test_url, headers=headers)
            return response.status_code == 200
        except:
            return False
    
    def make_api_call(self, url, method='GET', data=None, headers=None):
        """Make authenticated API call using cookies and CSRF"""
        logger.info(f"Making API call to {url}")
        
        # Ensure we're authenticated
        if not self.ensure_authenticated():
            logger.error("Authentication failed")
            return None
        
        # Log current cookies before making request
        logger.info(f"Current cookies: {list(self.requests_session.cookies.keys())}")
        
        # Prepare headers exactly like browser
        call_headers = {
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br, zstd',
            'accept-language': 'en-US,en;q=0.9',
            'connection': 'keep-alive',
            'content-type': 'application/json;charset=UTF-8',
            'origin': 'https://app.constructconnect.com',
            'referer': 'https://app.constructconnect.com/results?area=project&selectedContexts=details',
            'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
            'X-CSRF-Token': self.csrf_token,  # Try X-CSRF-Token header instead
            'csrf-token': self.csrf_token     # Keep this one too
        }
        
        if headers:
            call_headers.update(headers)
        
        try:
            logger.info(f"Making {method} request with CSRF token: {self.csrf_token[:10] if self.csrf_token else 'None'}...")
            
            if method.upper() == 'GET':
                response = self.requests_session.get(url, headers=call_headers)
            elif method.upper() == 'POST':
                response = self.requests_session.post(url, json=data, headers=call_headers)
            elif method.upper() == 'PUT':
                response = self.requests_session.put(url, json=data, headers=call_headers)
            elif method.upper() == 'DELETE':
                response = self.requests_session.delete(url, headers=call_headers)
            else:
                logger.error(f"Unsupported HTTP method: {method}")
                return None
                
            logger.info(f"Response status: {response.status_code}")
            
            # If 403, log more details but don't retry (we already ensure fresh auth above)
            if response.status_code == 403:
                logger.error(f"403 Forbidden - Response content: {response.text[:200]}")
                logger.error(f"Request URL: {response.url}")
                
                # Check if cookies were actually sent
                sent_cookies = response.request.headers.get('Cookie', '')
                logger.error(f"Cookies sent: {sent_cookies[:100] if sent_cookies else 'None'}")
                logger.error(f"CSRF token sent: {call_headers.get('csrf-token', 'None')}")
                exit()
                return None
            elif response.status_code == 400 or response.status_code == 400 or response.status_code == 404:
                logger.error(f"Terminated- cos API responded: {response.status_code}")
                exit()
            
            response.raise_for_status()
            logger.info(f"API call successful - Status: {response.status_code}")
            
            # Try to return JSON, fallback to text if not JSON
            try:
                return response.json()
            except:
                return response.text if response.content else None
            
        except Exception as e:
            logger.error(f"API call failed: {e}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response content: {e.response.text[:500]}")
            return None
    
    def test_simple_api_call(self):
        """Test a simpler API endpoint first"""
        simple_url = f"{self.app_url}/api/user/profile"
        logger.info("Testing simple API call to user profile")
        return self.make_api_call(simple_url, method='GET')