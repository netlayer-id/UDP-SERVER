# High-Performance PHP UDP Server (Lightweight)

A high-performance, event-driven UDP server implementation for PHP. This library leverages **Multiprocessing (Forking)**, **Non-blocking I/O**, and a custom **Timer Engine** to handle heavy network workloads with minimal overhead.

Perfect for building RADIUS servers, log aggregators, IoT gateways, or real-time monitoring tools.

## Key Features

*   **Multi-Worker Architecture**: Uses `pcntl_fork` to scale across multiple CPU cores, allowing parallel packet processing.
*   **UDP Port Sharing (SO_REUSEPORT)**: Enables multiple workers to bind to the same port, letting the kernel handle load balancing efficiently.
*   **Integrated Timer System**: Manage scheduled tasks (one-shot or recurring) within the event loop without blocking I/O.
*   **Master-Worker Supervision**: The master process monitors child workers and automatically restarts them if they exit or crash.
*   **High Efficiency**: Built with `socket_select` for low CPU usage during idle periods and high throughput under load.
*   **Zero Dependencies**: Pure PHP implementation—no external libraries or Composer packages required.

## System Requirements

*   PHP 7.4 or higher (PHP 8.x recommended).
*   PHP Extensions: `sockets`, `pcntl`, and `posix`.
*   A Unix-based OS (Linux, macOS, or WSL2 on Windows).

## Quick Start

### 1. Installation
Simply copy the `UDPServer.php` file into your project's class directory.

### 2. Basic Server Setup
Create a `server.php` file and implement the logic:
```php
require_once 'class/UDPServer.php';

$server = new UDPServer('0.0.0.0', 3000);

// Configure workers based on your CPU cores
$server->setWorkers(4)
$server->setReusePort(true);

// Define logic for incoming data
$server->onMessage(function($data, $ip, $port, $server) {
    echo "[$ip:$port] Received: " . trim($data) . "\n";
    
    // Send a response back to the client
    $server->sendTo("Data Received!", $ip, $port);
});

// Start the event loop
$server->start();
