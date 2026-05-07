<?php
/**
 * Author: NETLAYER
 * Website: https://netlayer.id
 * Description: High Performance UDP Server (Lightweight)
 * Function: Multiprocessing, Event Loop, and Timer Management
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
    
    private $timers = [];
    private $nextTimerId = 0;
    private $useReusePort = true;

    public function __construct($host = '0.0.0.0', $port = 3000)
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

    public function setReusePort($enabled)
    {
        $this->useReusePort = (bool)$enabled;
        return $this;
    }

    public function onMessage($callback) { $this->onMessageCallback = $callback; return $this; }
    public function onWorkerStart($callback) { $this->onWorkerStartCallback = $callback; return $this; }
    public function onWorkerStop($callback) { $this->onWorkerStopCallback = $callback; return $this; }

    // ==================== TIMER ENGINE ====================

    public function addTimer($interval, $callback, $repeat = false)
    {
        $id = ++$this->nextTimerId;
        $this->timers[$id] = [
            'id' => $id,
            'interval' => $interval,
            'callback' => $callback,
            'repeat' => $repeat,
            'next_run' => microtime(true) + $interval,
            'active' => true
        ];
        return $id;
    }

    private function getNextTimerTimeout()
    {
        if (empty($this->timers)) return 1.0; // Default sleep 1s if no timers
        
        $now = microtime(true);
        $min = 1.0;
        foreach ($this->timers as $timer) {
            $diff = $timer['next_run'] - $now;
            if ($diff < $min) $min = max(0, $diff);
        }
        return $min;
    }

    private function processTimers()
    {
        if (empty($this->timers)) return;
        $now = microtime(true);
        foreach ($this->timers as $id => &$timer) {
            if ($now >= $timer['next_run']) {
                call_user_func($timer['callback'], $id);
                if ($timer['repeat']) {
                    $timer['next_run'] = microtime(true) + $timer['interval'];
                } else {
                    unset($this->timers[$id]);
                }
            }
        }
    }

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

    public function sendTo($data, $ip, $port)
    {
        if ($this->socketClosed || !$this->socket) return false;
        return @socket_sendto($this->socket, $data, strlen($data), 0, $ip, $port);
    }

    private function runEventLoop()
    {
        // Optimasi: Alokasi buffer sekali di luar loop
        $buffer = '';
        $fromIp = '';
        $fromPort = 0;

        while ($this->running) {
            $read = [$this->socket];
            $write = $except = null;
            $timeout = $this->getNextTimerTimeout();
            
            // Konversi ke Sec dan USec
            $tv_sec = floor($timeout);
            $tv_usec = ($timeout - $tv_sec) * 1000000;

            // Blocking di level kernel (0% CPU saat idle)
            $num = @socket_select($read, $write, $except, (int)$tv_sec, (int)$tv_usec);

            pcntl_signal_dispatch();
            $this->processTimers();

            if ($num > 0) {
                // RADIUS packet jarang > 4096 bytes
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
            // Child process
            $this->workerPids = []; // Worker tidak perlu list saudaranya
            $this->runWorker($workerId);
            exit(0);
        } else {
            // Master process
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

        // Override signal handler untuk worker
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
            
            // Pantau worker yang mati (tanpa blocking)
            while (($pid = pcntl_wait($status, WNOHANG)) > 0) {
                $workerId = array_search($pid, $this->workerPids);
                if ($workerId !== false) {
                    echo "[MASTER] Worker #{$workerId} (PID {$pid}) exited. Respawning...\n";
                    unset($this->workerPids[$workerId]);
                    usleep(100000); // Backoff sedikit agar tidak spamming fork
                    $this->forkWorker($workerId);
                }
            }
            usleep(200000); // Cek setiap 200ms
        }
        $this->stopAllWorkers();
    }

    private function stopAllWorkers()
    {
        echo "\n[MASTER] Stopping all workers...\n";
        foreach ($this->workerPids as $pid) {
            @posix_kill($pid, SIGTERM);
        }
        
        // Beri waktu 1 detik untuk shutdown bersih
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
        // Biarkan SIGCHLD agar pcntl_wait bekerja
        pcntl_signal(SIGCHLD, function() {}); 
    }

    public function handleSignal($signal)
    {
        $this->running = false;
    }

    private function printBanner()
    {
        echo "\n+--------------------------------------------------+\n";
        echo "|        OPTIMIZED HIGH PERFORMANCE UDP            |\n";
        echo "+--------------------------------------------------+\n";
        echo "  Listen: {$this->host}:{$this->port}\n";
        echo "  Workers: {$this->workers} | SO_REUSEPORT: " . ($this->useReusePort ? 'ON' : 'OFF') . "\n";
        echo "----------------------------------------------------\n\n";
    }
}
