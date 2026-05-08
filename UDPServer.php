<?php
/**
 * Author: NETLAYER
 * Website: https://netlayer.id
 * Description: High Performance UDP Server (Cleaned)
 * Function: Multiprocessing, Event Loop (Data Transmission Focus)
 */
class UDPServer
{
    private $host;
    private $port;
    private $workers = 1;
    private $socket;
    private $socketClosed = false;
    
    private $masterPid;
    private $workerPids = [];
    private $running = true;
    
    private $onMessageCallback;
    private $onWorkerStartCallback;
    private $onWorkerStopCallback;
    private $useReusePort = true;

    public function __construct($host = '0.0.0.0', $port = 1812)
    {
        $this->host = $host;
        $this->port = $port;
    }

    public function __destruct()
    {
        $this->closeSocket();
    }

    private function closeSocket()
    {
        if (!$this->socketClosed && $this->socket) {
            @socket_close($this->socket);
            $this->socketClosed = true;
        }
    }

    // ==================== CONFIGURATION ====================

    public function setWorkers($count)
    {
        $this->workers = max(1, (int)$count);
        return $this;
    }

    public function onPacket($callback) { $this->onMessageCallback = $callback; return $this; }
    public function onWorkerStart($callback) { $this->onWorkerStartCallback = $callback; return $this; }
    public function onWorkerStop($callback) { $this->onWorkerStopCallback = $callback; return $this; }

    // ==================== NETWORK CORE ====================

    private function createSocket()
    {
        $this->socket = socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
        if (!$this->socket) throw new Exception("Socket creation failed");

        socket_set_option($this->socket, SOL_SOCKET, SO_REUSEADDR, 1);
        if ($this->useReusePort && defined('SO_REUSEPORT')) {
            @socket_set_option($this->socket, SOL_SOCKET, SO_REUSEPORT, 1);
        }

        // Buffer 8MB untuk trafik padat
        @socket_set_option($this->socket, SOL_SOCKET, SO_RCVBUF, 8 * 1024 * 1024);
        @socket_set_option($this->socket, SOL_SOCKET, SO_SNDBUF, 8 * 1024 * 1024);
        
        socket_set_nonblock($this->socket);

        if (!@socket_bind($this->socket, $this->host, $this->port)) {
            throw new Exception("Bind failed: " . socket_strerror(socket_last_error($this->socket)));
        }
        $this->socketClosed = false;
    }

    public function send($data, $ip, $port)
    {
        if ($this->socketClosed || !$this->socket) return false;
        return @socket_sendto($this->socket, $data, strlen($data), 0, $ip, $port);
    }

    private function runEventLoop()
    {
        $buffer = '';
        $fromIp = '';
        $fromPort = 0;

        while ($this->running) {
            $read = [$this->socket];
            $write = $except = null;
            
            // Menggunakan timeout statis 1 detik untuk efisiensi CPU saat idle
            $num = @socket_select($read, $write, $except, 1, 0);

            pcntl_signal_dispatch();

            if ($num > 0) {
                // RADIUS packet rarely > 4096 bytes
                $bytes = @socket_recvfrom($this->socket, $buffer, 8192, 0, $fromIp, $fromPort);
                if ($bytes > 0 && $this->onMessageCallback) {
                    call_user_func($this->onMessageCallback, $buffer, $fromIp, $fromPort, $this);
                }
            }
        }
    }

    // ==================== PROCESS MGMT ====================

    public function start()
    {
        $this->masterPid = posix_getpid();
        $this->printBanner();
        $this->setupSignals();
        $this->createSocket();

        for ($i = 0; $i < $this->workers; $i++) {
            $this->forkWorker($i);
        }

        $this->monitorWorkers();
    }

    private function forkWorker($workerId)
    {
        $pid = pcntl_fork();
        if ($pid == -1) throw new Exception("Fork failed");

        if ($pid == 0) {
            $this->workerPids = []; 
            $this->runWorker($workerId);
            exit(0);
        } else {
            $this->workerPids[$workerId] = $pid;
            echo "[MASTER] Worker #{$workerId} launched (PID: {$pid})\n";
        }
    }

    private function runWorker($workerId)
    {
        $pid = posix_getpid();
        if ($this->onWorkerStartCallback) {
            call_user_func($this->onWorkerStartCallback, $workerId, $pid, $this);
        }

        pcntl_signal(SIGINT, [$this, 'handleSignal']);
        pcntl_signal(SIGTERM, [$this, 'handleSignal']);

        $this->runEventLoop();

        if ($this->onWorkerStopCallback) {
            call_user_func($this->onWorkerStopCallback, $workerId, $pid);
        }
        $this->closeSocket();
    }

    private function monitorWorkers()
    {
        while ($this->running) {
            pcntl_signal_dispatch();
            
            while (($pid = pcntl_wait($status, WNOHANG)) > 0) {
                $workerId = array_search($pid, $this->workerPids);
                if ($workerId !== false) {
                    echo "[MASTER] Worker #{$workerId} (PID {$pid}) exited. Respawning...\n";
                    unset($this->workerPids[$workerId]);
                    usleep(100000); 
                    $this->forkWorker($workerId);
                }
            }
            usleep(200000); 
        }
        $this->stopAllWorkers();
    }

    private function stopAllWorkers()
    {
        echo "\n[MASTER] Stopping all workers...\n";
        foreach ($this->workerPids as $pid) {
            @posix_kill($pid, SIGTERM);
        }
        
        usleep(1000000);
        
        foreach ($this->workerPids as $pid) {
            pcntl_waitpid($pid, $status, WNOHANG);
        }
        $this->closeSocket();
        echo "[MASTER] Goodbye.\n";
    }

    private function setupSignals()
    {
        pcntl_signal(SIGINT, [$this, 'handleSignal']);
        pcntl_signal(SIGTERM, [$this, 'handleSignal']);
        pcntl_signal(SIGQUIT, [$this, 'handleSignal']);
        pcntl_signal(SIGCHLD, function() {}); 
    }

    public function handleSignal($signal)
    {
        $this->running = false;
    }

    private function printBanner()
    {
        echo "\n+--------------------------------------------------+\n";
        echo "|        HIGH PERFORMANCE UDP SERVER         |\n";
        echo "+--------------------------------------------------+\n";
        echo "  Listen: {$this->host}:{$this->port}\n";
        echo "  Workers: {$this->workers} | SO_REUSEPORT: " . ($this->useReusePort ? 'ON' : 'OFF') . "\n";
        echo "----------------------------------------------------\n\n";
    }
}
