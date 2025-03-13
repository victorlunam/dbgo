# DBGo

DBGo is a command-line tool for comparing SQL Server databases with a beautiful terminal user interface. It helps you identify differences between source and target databases efficiently.

## Features

- [x] Interactive terminal user interface
- [x] SQL Server database comparison
- [ ] Visual diff highlighting
- [x] Easy configuration through JSON
- [x] Cross-platform support

## Prerequisites

- Go 1.23.5 or later
- SQL Server instance(s)
- Access to source and target databases

## Installation

1. Clone the repository:
```bash
git clone https://github.com/victorlunam/dbgo.git
cd dbgo
```

2. Install dependencies:
```bash
go mod download
```

3. Build the project:
```bash
go build -o dbgo ./cmd/main.go
```

## Configuration

1. Copy the sample configuration file:
```bash
cp godb.config.sample.json godb.config.json
```

2. Edit `godb.config.json` with your database connection details:
```json
{
  "source": {
    "server": "your-source-server",
    "port": "1433",
    "database": "your-source-db",
    "user": "your-username",
    "password": "your-password"
  },
  "target": {
    "server": "your-target-server",
    "port": "1433",
    "database": "your-target-db",
    "user": "your-username",
    "password": "your-password"
  }
}
```

## Usage

Run the application:
```bash
./dbgo
```

Run the application with logging enabled:
```bash
./dbgo -l
```

The application will start with an interactive terminal interface where you can:
- Navigate through database objects
- Compare schemas
- Export results

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 