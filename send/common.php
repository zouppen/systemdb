<?php

const EOF = false;
const TIMEOUT = true;
const NOT_JSON = null;

error_reporting(E_ALL | E_STRICT);

// 1 hour travel to the past is allowed (fake-hwclock update interval)
$allowed_time_travel = "-3600";

// Let's have a global notices-to-errors thingy
set_error_handler(function($severity, $message, $file, $line) {
    if (!(error_reporting() & $severity)) {
        // This error code is not included in error_reporting
        return false;
    }

    // Check if it's an E_NOTICE
    if ($severity === E_NOTICE) {
        // Convert the notice to an exception and throw it
        throw new ProcessingException($message);
    }

    // Dump the rest as errors with stack trace (not catching them)
    throw new ErrorException($message, 0, $severity, $file, $line);
});

function getline_timeout($stream, $timeout, $stream_remote)
{
    // Split the time 32-bit safe way
    if ($timeout === null) {
        // Blocking read with no timeout
        $sec = null;
        $ms = null;
        $skip_read = false;
    } else if (bccomp("0", $timeout, 9) === 1) {
        // Timeout imminent. Don't even try to get new data, just
        // non-blockingly check if the remote pipe is still fine.
        $sec = 0;
        $ms = 0;
        $skip_read = true;
    } else {
        // Blocking read with timeout
        $sec = intval($timeout);
        $ms = intval(bcmul(bcsub($timeout, $sec, 9), "1000000", 9));
        $skip_read = false;
    }

    $inputs = $skip_read ? [$stream_remote] : [$stream, $stream_remote];
    $write = [];
    $except = [];
    if (stream_select($inputs, $write, $except, $sec, $ms)) {
        foreach ($inputs as $i) {
            if ($i === $stream_remote) {
                $s = fread($stream_remote, 1024);
                if (feof($stream_remote)) {
                    throw new ProcessingException("Remote stream closed", 2);
                } else {
                    throw new OtherMessage("Unexpected input received from remote", $s);
                }
            }
        }
        // This is just a new line
        return fgets($stream);
    } else {
        return TIMEOUT;
    }
}

function hrtime_sec() {
    [$s, $ns] = hrtime();
    return bcadd($s, bcdiv($ns, "1000000000", 9), 9);
}

function pipe_period($stream, $period, $line_func, $period_func, $stream_remote)
{
    $next_tick = null;

    while(true) {
        $left = $next_tick === null ? null : bcsub($next_tick, hrtime_sec(), 9);
        try {
            $line = getline_timeout($stream, $left, $stream_remote);
            if ($line === EOF) {
                return;
            } elseif ($line === TIMEOUT) {
                $next_tick = null;
                $period_func();
            } else {
                if ($next_tick === null) {
                    $next_tick = bcadd(hrtime_sec(), $period, 9);
                }
                $line_func($line);
            }
        } catch (SkipMessage $e) {
            $e->warn();
        } catch (otherMessage $e) {
            $e->warn();
        }
    }
}

function journalctl_sanity_check($stream, $expected, $ts)
{
    global $allowed_time_travel;

    // On initial run, don't discard any input
    if ($expected === '') return;

    // Testing that we can get data
    $line = fgets($stream);
    if ($line === false) {
        throw new ProcessingException('Unable to fetch first journal row');
    }

    // Test that it's JSON
    $data = json_decode($line, true);
    if ($data == NOT_JSON) {
        throw new ProcessingException('Garbage received from journalctl');
    }

    // .. and is contiguous
    if ($data['__CURSOR'] !== $expected) {
        $clock_diff = bcdiv(bcsub($data['__REALTIME_TIMESTAMP'], $ts, 0), 1000000, 3);

        if (bccomp($clock_diff, $allowed_time_travel, 3) === 1) {
            fprintf(STDERR, "Some rows are lost after cursor %s. Time difference is %s seconds. Skipping.\n", $expected, $clock_diff);
        } else {
            throw new ProcessingException('Unexpected cursor: ' . $data['__CURSOR']);
        }
    }
}

function journalctl_single($stream, $follow, $cmdline_extra, $cursor_start, $cursor_start_ts, $f, $stream_remote)
{
    // Convert cursor to cmd
    $after = $cursor_start === '' ? '' : escapeshellarg('--cursor='.$cursor_start);
    $follow_arg = $follow ? '-f' : '';

    $res = proc_open("exec journalctl -qa --no-tail -o json $after $follow_arg $cmdline_extra", [
        ['pipe', 'r'], // stdin
        ['pipe', 'w'], // stdout
        STDERR, // dump stdout to console
    ], $pipes);
    if ($res == false) {
        throw new ProcessingException('Unable to start journalctl');
    }
    fclose($pipes[0]); // stdin not used

    // Make sure we are getting correct data
    journalctl_sanity_check($pipes[1], $cursor_start, $cursor_start_ts);
    fwrite(STDERR, $follow ?
           "Backfill done, syncing real-time\n" :
           "Journal backfill started\n"
    );

    $datafunc = function($line) use ($stream, &$cursor, &$cursor_ts, $f) {
        $json = json_decode($line, true);
        if ($json == NOT_JSON) {
            throw new SkipMessage("Skipping journalctl garbage");
        } else {
            $cursor = $json['__CURSOR'];
            $cursor_ts = $json['__REALTIME_TIMESTAMP'];
            try {
                $ts = bcdiv($json['__REALTIME_TIMESTAMP'], 1000000, 3);
                $output = $f($json, $ts);
                if ($output === null) {
                    throw new SkipMessage("Fallthrough from user function");
                }
                fputcsv($stream, $output, escape: "");
            } catch (SkipMessage $e) {
                // Add some context
                $e->setCursor($cursor);
                throw $e;
            }
        }
    };
    $cursor_func = function() use ($stream, &$cursor, &$cursor_ts) {
        if ($cursor === null) return; // Skip if we haven't got anything yet
        fputcsv($stream, ['_', $cursor, $cursor_ts], escape: "");
    };

    // When back-filling data, the commit interval may be way longer.
    $timeout = $follow ? 3 : 20;
    pipe_period($pipes[1], $timeout, $datafunc, $cursor_func, $stream_remote);

    // After EOF, report last cursor and return it to the caller.
    $cursor_func();

    // Make sure it died gracefully
    $exitcode = proc_close($res);
    if ($exitcode !== 0) {
        throw new ProcessingException("journalctl failed (exit code: $exitcode)", 3);
    }

    return $cursor ?? $cursor_start;
}

// Version of journalctl which dies with a more compact message if
// processing error occurs.
function journalctl_catch()
{
    try {
        return call_user_func_array("journalctl", func_get_args());
    } catch (ProcessingException $e) {
        $e->die();
    }
}

function journalctl($command, $hello, $cmdline_extra, $f)
{
    $res = proc_open($command, [
        ['pipe', 'r'], // stdin
        ['pipe', 'w'], // stdout
        STDERR, // dump stdout to console
    ], $pipes);
    if ($res == false) {
        throw new ProcessingException('Unable to start data sink command');
    }

    if (fwrite($pipes[0], "$hello\n") === false) {
        throw new ProcessingException('Error in hello phase');
    }

    $cursor = fgets($pipes[1]);
    if ($cursor === false) {
        throw new ProcessingException('No remote cursor received');
    }
    $cursor = trim($cursor);

    $cursor_ts = fgets($pipes[1]);
    if ($cursor_ts === false) {
        throw new ProcessingException('No remote timestamp received');
    }
    $cursor_ts = trim($cursor_ts);

    fprintf(STDERR, "Connected\n");

    // Run journal reader twice, first without follow and then with
    // follow on. Helps to mitigate certain journald issues when
    // following to months-old log files.
    $cursor = journalctl_single($pipes[0], false, $cmdline_extra, $cursor, $cursor_ts, $f, $pipes[1]);
    journalctl_single($pipes[0], true, $cmdline_extra, $cursor, $cursor_ts, $f, $pipes[1]);
}

class SkipMessage extends Exception
{
    private $cursor;

    public function __construct($message)
    {
        parent::__construct($message, 0);
    }

    public function setCursor($cursor) {
        $this->cursor = $cursor;
    }

    public function warn()
    {
        fprintf(STDERR, "Skipping a log message, reason: %s. Cursor: %s\n", $this->getMessage(), $this->cursor);
    }
}

class OtherMessage extends Exception
{
    private $payload;

    public function __construct($message, $payload)
    {
        parent::__construct($message, 0);
        $this->payload = $payload;
    }

    public function warn()
    {
        fprintf(STDERR, "%s: %s\n", $this->getMessage(), json_encode($this->payload));
    }
}

class ProcessingException extends Exception
{
    private $exitCode;

    public function __construct($message, $exitCode=1)
    {
        $this->exitCode = $exitCode;
        parent::__construct($message, 0);
    }

    public function die()
    {
        fprintf(STDERR, "Fatal error: %s\n", $this->getMessage());
        exit($this->exitCode);
    }
}
