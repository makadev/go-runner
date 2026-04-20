# vscode-go-module

This is a simple example of a Go module project that can be used with Visual Studio Code.

## Package Setup

Remove the existing `go.mod` and `go.sum` as well as the `*.go` files and create a new module.

```
rm -f go.mod go.sum
rm -f *.go
echo "package main" > main.go
echo "package main" > main_test.go
go mod init github.com/your-username/your-repo
go mod tidy
```

Replace README.md with your own content.
