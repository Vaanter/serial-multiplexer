# Serial Multiplexer

A command line tool that allows tunnelling/proxying TCP traffic through a VirtualBox VM using Windows named pipe(s)
and serial port(s).

# Getting started

## Prerequisites

- A Windows host, Linux is not supported
- VirtualBox with Linux or Windows VM (Tested with Ubuntu 24 TLS, Fedora 42 Xfce spin and Windows 10 and 11)

## Usage

This application is runnable in two modes:

- host - This mode listens for connections and proxies data between client programs (e.g. Google Chrome, DBeaver, etc.)
  and the VM via [Windows Named Pipes](https://learn.microsoft.com/en-us/windows/win32/ipc/named-pipes)
- guest - This mode is used inside the VM to read and write data between serial port(s) and a target server (e.g.
  Database, Website, etc.)

### VirtualBox setup

- [Set up a Linux/Windows VM](https://www.virtualbox.org/manual/topics/Introduction.html#create-vm-wizard)
- In the VirtualBox VM settings switch to the Expert tab
- Create a serial port:
    - Enable the port
    - Port number and COM numbers should match. Meaning port 1 uses COM1, port 2 - COM2, port3 - COM3 and port4 - COM4
    - Port Mode: Host Pipe
    - Connect to existing pipe/socket - unchecked
    - Path/Address: Pipe path MUST start with "\\\\.\\pipe\\" followed by a custom name e.g.
      \\\\.\\pipe\\myAmazingPipe123
- Multiple serial ports can be used concurrently

### Host setup

**Host mode can only be used on Windows!**

1. Download a prebuilt binary from [Releases](https://github.com/Vaanter/serial-multiplexer/releases/)
2. Create a config file. A sample with supported configuration keys is provided in the repository
   ([config.toml](https://github.com/Vaanter/serial-multiplexer/blob/main/config.toml))
3. Start the VM
4. Start the proxy in host mode:
    ```shell
    # --pipe-path parameter can be repeated to use multiple pipes
    ./serial-multiplexer-0.4.0-windows.exe host --pipe-path "\\.\pipe\myAmazingPipe123"
    ```

### Guest setup

1. Download a prebuilt binary from [Releases](https://github.com/Vaanter/serial-multiplexer/releases/)
2. (Optional) Create a config file. A sample with supported configuration keys is provided in the repository
   ([config.toml](https://github.com/Vaanter/serial-multiplexer/blob/main/config.toml))
3. (Linux only) Set the downloaded binary as executable:
   ```shell
   chmod +x serial-multiplexer-0.4.0-linux
   ```
4. Start the proxy in guest mode (requires sudo on Linux):
   ```shell
   # --serial-port parameter can be repeated to use multiple serial ports
   # Linux
   sudo ./serial-multiplexer-0.4.0-linux --serial-port "/dev/ttyS0"
   # Windows
   ./serial-multiplexer-0.4.0-windows.exe guest --serial-port "COM1"
   ```

### Connecting

Applications can connect through the proxy in two ways:

- The application connects directly to the multiplexer.
  An address pair must be specified in the config file.
  This pair consists of a listener address - the IP:port combination on which the multiplexer listens and
  where the application connects and a target address - the IP/hostname:port to which the guest in VM will connect.
- Using the multiplexer as a Socks5 proxy.
  The client application must support and be configured to connect via socks5.
  A socks5 proxy listener must be specified in the config file.

### Additional info

Sometimes the Windows Pipe might be occupied by another process preventing its use.
Restarting the VM usually helps.
