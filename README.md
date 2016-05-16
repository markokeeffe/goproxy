# goproxy

## Usage

### Linux / OSX

#### Build From Source

```bash
    go build goproxy.go
```

#### Run as Service

Install the service:
```bash
    sudo goproxy -service install
```

Start the service:
```bash
    sudo goproxy -key=ABCD123 -service start
```

Stop the service:
```bash
    sudo goproxy -key=ABCD123 -service stop
```

Restart the service:
```bash
    sudo goproxy -key=ABCD123 -service restart
```

Uninstall the service:
```bash
    sudo goproxy -service uninstall
```


### Windows

#### Build From Source

```bash
    GOOS=windows GOARCH=386 go build -o goproxy.exe goproxy.go
```

#### Run as Service

Install the service:
```bash
    sudo goproxy.exe -service install
```

Start the service:
```bash
    sudo goproxy.exe -key=ABCD123 -service start
```

Stop the service:
```bash
    sudo goproxy.exe -key=ABCD123 -service stop
```

Restart the service:
```bash
    sudo goproxy.exe -key=ABCD123 -service restart
```

Uninstall the service:
```bash
    sudo goproxy.exe -service uninstall
```
