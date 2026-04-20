# vscode-go-module

This is a simple example of a Go module project that can be used with Visual Studio Code.

## Package Setup

Remove the existing `go.mod` and `go.sum` as well as the `*.go` files and create a new module.

```
rm -f go.mod go.sum
rm -f *.go
echo "package runner" > main.go
echo "package runner" > main_test.go
go mod init github.com/makadev/go-runner
go mod tidy
```

Replace README.md with your own content.
