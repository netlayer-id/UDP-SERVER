<?php
/**
 * Author: NETLAYER
 * Website: https://netlayer.id
 * Description: High Performance UDP Server (Lightweight)
 * Function: Multiprocessing, Event Loop, and Timer Management
 */
class UDPServer
{
    // Configuration
    private $host;
    private $port;
    private $workers = 1;
    private $socket;
    private $socketClosed = false;
    
    // Process IDs
    private $masterPid;
    private $workerPids = [];
    
    // Status
    private $running = true;
    
    // Callbacks
    private $onMessageCallback;
    private $onWorkerStartCallback;
    private $onWorkerStopCallback;
    
    // Timers
    private $timers = [];
    private $nextTimerId = 0;
    
    // High performance flags
    private $useReusePort = true;
    
    // Long running protection
    private $loopCount = 0;
    
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
        $this->workers = (int)$count;
        return $this;
    }
    
    public function setReusePort($enabled)
    {
        $this->useReusePort = $enabled;
        return $this;
    }
    
    public function onMessage($callback)
    {
        $this->onMessageCallback = $callback;
        return $this;
    }
    
    public function onWorkerStart($callback)
    {
        $this->onWorkerStartCallback = $callback;
        return $this;
    }
    
    public function onWorkerStop($callback)
    {
        $this->onWorkerStopCallback = $callback;
        return $this;
    }
    
    // ==================== TIMER ====================
    
    public function addTimer($interval, $callback, $repeat = false)
    {
        $this->nextTimerId++;
        $id = $this->nextTimerId;
        
        $this->timers[$id] = [
            'id' => $id,
            'interval' => $interval,
            'callback' => $callback,
            'repeat' => $repeat,
            'first_run' => microtime(true),
            'next_run' => microtime(true) + $interval,
            'counter' => 0,
            'active' => true
        ];
        
        return $id;
    }
    
    public function clearTimer($id)
    {
        if (isset($this->timers[$id])) {
            $this->timers[$id]['active'] = false;
            unset($this->timers[$id]);
        }
        return $this;
    }
    
    private function getNextTimerTimeout()
    {
        $now = microtime(true);
        $nextTimeout = 1.0;
        
        foreach ($this->timers as $timer) {
            if (!$timer['active']) continue;
            $timeout = $timer['next_run'] - $now;
            if ($timeout < $nextTimeout && $timeout > 0) {
                $nextTimeout = $timeout;
            }
        }
        
        return max($nextTimeout, 0.001);
    }
    
    private function processTimers()
    {
        $now = microtime(true);
        
        foreach ($this->timers as $id => &$timer) {
            if (!$timer['active']) continue;
            
            if ($now >= $timer['next_run']) {
                call_user_func($timer['callback'], $id);
                
                if ($timer['repeat']) {
                    $timer['counter']++;
                    $timer['next_run'] = $timer['first_run'] + ($timer['interval'] * $timer['counter']);
                    
                    if ($timer['next_run'] < $now) {
                        $timer['first_run'] = $now;
                        $timer['counter'] = 0;
                        $timer['next_run'] = $now + $timer['interval'];
                    }
                } else {
                    unset($this->timers[$id]);
                }
            }
        }
        
        // Cleanup old timers every hour
        static $lastCleanup = 0;
        if ($now - $lastCleanup > 3600) {
            foreach ($this->timers as $id => $timer) {
                if (!$timer['active'] || ($now - $timer['first_run'] > 86400)) {
                    unset($this->timers[$id]);
                }
            }
            $lastCleanup = $now;
        }
    }
    
    // ==================== SOCKET ====================
    
    private function createSocket()
    {
        $this->socket = socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
        
        if (!$this->socket) {
            throw new Exception("Cannot create socket: " . socket_strerror(socket_last_error()));
        }
        
        $this->socketClosed = false;
        
        socket_set_option($this->socket, SOL_SOCKET, SO_REUSEADDR, 1);
        
        if ($this->useReusePort && defined('SO_REUSEPORT')) {
            @socket_set_option($this->socket, SOL_SOCKET, SO_REUSEPORT, 1);
        }
        
        @socket_set_option($this->socket, SOL_SOCKET, SO_RCVBUF, 1024 * 1024);
        @socket_set_option($this->socket, SOL_SOCKET, SO_SNDBUF, 1024 * 1024);
        
        socket_set_nonblock($this->socket);
        
        if (!socket_bind($this->socket, $this->host, $this->port)) {
            throw new Exception("Cannot bind to {$this->host}:{$this->port}: " . socket_strerror(socket_last_error($this->socket)));
        }
    }
    
    public function sendTo($data, $ip, $port)
    {
        if ($this->socketClosed || !$this->socket) {
            return false;
        }
        
        return socket_sendto($this->socket, $data, strlen($data), 0, $ip, $port);
    }
    
    // ==================== EVENT LOOP ====================
    
    private function runEventLoop()
    {
        while ($this->running) {
            if ($this->socketClosed) break;
            
            $read = [$this->socket];
            $write = null;
            $except = null;
            
            $timeoutUsec = (int)($this->getNextTimerTimeout() * 1000000);
            $timeoutSec = 0;
            
            $num = @socket_select($read, $write, $except, $timeoutSec, $timeoutUsec);
            
            if ($num === false) {
                $error = socket_last_error();
                if ($error !== 4) {
                    echo "socket_select error: " . socket_strerror($error) . "\n";
                }
                socket_clear_error();
                pcntl_signal_dispatch();
                $this->processTimers();
                continue;
            }
            
            pcntl_signal_dispatch();
            $this->processTimers();
            
            if ($num > 0) {
                $buffer = '';
                $fromIp = '';
                $fromPort = 0;
                
                $bytes = @socket_recvfrom($this->socket, $buffer, 65535, 0, $fromIp, $fromPort);
                
                if ($bytes > 0 && $this->onMessageCallback) {
                    call_user_func($this->onMessageCallback, $buffer, $fromIp, $fromPort, $this);
                }
            }
            
            // Prevent CPU overload on idle
            $this->loopCount++;
            if ($this->loopCount % 10000 == 0) {
                usleep(1000);
            }
        }
    }
    
    // ==================== PROCESS MANAGEMENT ====================
    
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
        
        if ($pid == -1) {
            throw new Exception("Failed to fork worker {$workerId}");
        }
        
        if ($pid == 0) {
            $this->runWorker($workerId);
            exit(0);
        }
        
        $this->workerPids[] = $pid;
        echo "[MASTER] Worker {$workerId} started (PID: {$pid})\n";
    }
    
    private function runWorker($workerId)
    {
        $this->running = true;
        $pid = posix_getpid();
        
        echo "[WORKER {$workerId}] Started (PID: {$pid})\n";
        
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
        echo "[WORKER {$workerId}] Stopped\n";
    }
    
    private function monitorWorkers()
    {
        while ($this->running) {
            pcntl_signal_dispatch();
            
            // Reap zombies
            while (($pid = pcntl_wait($status, WNOHANG)) > 0) {
                $key = array_search($pid, $this->workerPids);
                if ($key !== false) {
                    echo "[MASTER] Worker PID {$pid} died, restarting...\n";
                    unset($this->workerPids[$key]);
                    usleep(500000);
                    $this->forkWorker($key);
                }
            }
            
            usleep(100000);
        }
        
        $this->stopAllWorkers();
    }
    
    private function stopAllWorkers()
    {
        echo "\n[MASTER] Stopping all workers...\n";
        
        foreach ($this->workerPids as $pid) {
            @posix_kill($pid, SIGTERM);
        }
        
        foreach ($this->workerPids as $pid) {
            pcntl_waitpid($pid, $status, WNOHANG);
        }
        
        $this->closeSocket();
        echo "[MASTER] All workers stopped\n";
    }
    
    // ==================== SIGNAL HANDLER ====================
    
    private function setupSignals()
    {
        pcntl_signal(SIGINT, [$this, 'handleSignal']);
        pcntl_signal(SIGTERM, [$this, 'handleSignal']);
        pcntl_signal(SIGQUIT, [$this, 'handleSignal']);
        pcntl_signal(SIGCHLD, SIG_IGN);
    }
    
    public function handleSignal($signal)
    {
        $pid = posix_getpid();
        
        if ($pid == $this->masterPid) {
            echo "\n[MASTER] Shutting down...\n";
        } else {
            echo "[WORKER] Shutting down...\n";
        }
        
        $this->running = false;
    }
    
    // ==================== UI ====================
    
    private function printBanner()
    {
        echo "\n";
        echo "+--------------------------------------------------+\n";
        echo "|         HIGH PERFORMANCE UDP SERVER             |\n";
        echo "+--------------------------------------------------+\n";
        echo "|  Host:     {$this->host}:{$this->port}\n";
        echo "|  Workers:  {$this->workers}\n";
        echo "|  ReusePort: " . ($this->useReusePort ? 'Yes' : 'No') . "\n";
        echo "|  Master:   PID {$this->masterPid}\n";
        echo "+--------------------------------------------------+\n";
        echo "\n";
    }
}
