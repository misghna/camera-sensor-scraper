import serial
import serial.tools.list_ports
import time

def find_bluetooth_com_ports():
    """Find Bluetooth COM ports"""
    print("🔍 Scanning for COM ports...")
    
    ports = serial.tools.list_ports.comports()
    bt_ports = []
    
    for port in ports:
        description = port.description.lower()
        device = port.device.lower()
        
        print(f"Found: {port.device} - {port.description}")
        
        # Look for Bluetooth-related ports
        if any(keyword in description for keyword in ['bluetooth', 'bt', 'rfcomm', 'wireless']):
            bt_ports.append(port.device)
            print(f"   ✅ Potential Bluetooth port: {port.device}")
    
    return bt_ports

def test_com_port(port_name):
    """Test communication with a COM port"""
    print(f"\n🔌 Testing {port_name}...")
    
    try:
        # Try to open the port
        ser = serial.Serial(
            port=port_name,
            baudrate=9600,
            timeout=5,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            bytesize=serial.EIGHTBITS
        )
        
        if ser.is_open:
            print(f"   ✅ Opened {port_name}")
            
            # Send test message
            test_msg = "Hello from Windows!"
            print(f"   📤 Sending: {test_msg}")
            ser.write(f"{test_msg}\r\n".encode())
            ser.flush()
            
            # Wait for response
            time.sleep(2)
            if ser.in_waiting > 0:
                response = ser.read(ser.in_waiting).decode('utf-8', errors='ignore')
                print(f"   📨 Response: {response.strip()}")
                ser.close()
                return True
            else:
                print(f"   ⏰ No response from {port_name}")
                ser.close()
                return False
        else:
            print(f"   ❌ Could not open {port_name}")
            return False
            
    except Exception as e:
        print(f"   ❌ Error with {port_name}: {e}")
        return False

def interactive_serial_communication(port_name):
    """Interactive communication via serial"""
    print(f"\n💬 Starting interactive mode on {port_name}")
    print("Type 'quit' to exit")
    
    try:
        ser = serial.Serial(
            port=port_name,
            baudrate=9600,
            timeout=2,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            bytesize=serial.EIGHTBITS
        )
        
        while True:
            message = input("\nSend to Pi: ").strip()
            
            if message.lower() == 'quit':
                break
                
            if message:
                # Send message
                ser.write(f"{message}\r\n".encode())
                ser.flush()
                print(f"📤 Sent: {message}")
                
                # Wait for response
                time.sleep(1)
                if ser.in_waiting > 0:
                    response = ser.read(ser.in_waiting).decode('utf-8', errors='ignore')
                    print(f"📨 Pi: {response.strip()}")
                else:
                    print("⏰ No response")
        
        ser.close()
        
    except Exception as e:
        print(f"❌ Communication error: {e}")

def setup_bluetooth_com():
    """Help setup Bluetooth COM port"""
    print("\n📋 BLUETOOTH COM PORT SETUP GUIDE")
    print("=" * 35)
    print("If no Bluetooth COM ports were found, follow these steps:")
    print()
    print("1. Open Windows Settings → Bluetooth & devices")
    print("2. Find 'RaspberryPi' in your paired devices")
    print("3. Click on it → 'More Bluetooth options'")
    print("4. Go to 'COM Ports' tab")
    print("5. Click 'Add' → 'Outgoing' → Select RaspberryPi")
    print("6. Note the COM port number (e.g., COM5)")
    print("7. Run this script again")
    print()
    print("Alternatively, try Device Manager:")
    print("1. Open Device Manager")
    print("2. Look under 'Ports (COM & LPT)'")
    print("3. Find Bluetooth-related COM ports")

def main():
    print("🔗 Windows Bluetooth COM Client")
    print("=" * 32)
    
    # Check if pyserial is installed
    try:
        import serial
    except ImportError:
        print("❌ pyserial not installed")
        print("Run: pip install pyserial")
        return
    
    # Find Bluetooth COM ports
    bt_ports = find_bluetooth_com_ports()
    
    if not bt_ports:
        print("\n❌ No Bluetooth COM ports found")
        setup_bluetooth_com()
        
        # Manual COM port entry
        manual_port = input("\nEnter COM port manually (e.g., COM5) or press Enter to exit: ").strip()
        if manual_port:
            bt_ports = [manual_port.upper()]
        else:
            return
    
    # Test each port
    working_port = None
    for port in bt_ports:
        if test_com_port(port):
            working_port = port
            break
    
    if working_port:
        print(f"\n🎉 Found working Bluetooth port: {working_port}")
        
        mode = input("\nChoose mode:\n1. Interactive\n2. Quick test\nEnter choice: ").strip()
        
        if mode == "1":
            interactive_serial_communication(working_port)
        else:
            print("✅ Quick test completed successfully!")
    else:
        print("\n❌ No working Bluetooth COM ports found")
        setup_bluetooth_com()

if __name__ == "__main__":
    main()