# Serial Multiplexer

A command line tool that allows tunnelling/proxying TCP traffic through a VirtualBox VM using windows named pipe(s)
and serial port(s).

# Getting started

## Prerequisites

- A Windows host, Linux is not supported
- VirtualBox with a Linux VM (Tested with Ubuntu 24 TLS and Fedora 42 Xfce spin), a Windows VM might work (not tested)

## Usage

This application has two run modes:

- host - This mode listens for connections and proxies data between client programs (e.g. Google Chrome, DBeaver)
  and the VM via [Windows Named Pipes](https://learn.microsoft.com/en-us/windows/win32/ipc/named-pipes)
- guest - This mode is used inside the VM to read and write data between serial port(s) and a target server (e.g.
  Database, Website)

### VirtualBox setup

- [Set up a Linux VM](https://www.virtualbox.org/manual/topics/Introduction.html#create-vm-wizard)
- In the VirtualBox VM settings switch to the Expert tab
- Create a serial port:
    - Enable the port
    - Port number and COM numbers should match. Meaning port 1 uses COM1, port 2 - COM2, port3 - COM3 and port4 - COM4
    - Port Mode: Host Pipe
    - Connect to existing pipe/socket - unchecked
    - Path/Address: Pipe path MUST start with "\\\\.\pipe\" followed by a custom name e.g. \\\\.\pipe\myAmazingPipe123
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
3. Set the downloaded binary as executable:
   ```shell
   chmod +x serial-multiplexer-0.4.0-linux
   ```
4. Start the proxy in guest mode (requires sudo):
   ```shell
   # --serial-port parameter can be repeated to use multiple serial ports
   sudo ./serial-multiplexer-0.4.0-linux --serial-port "/dev/ttyS0"
   ```