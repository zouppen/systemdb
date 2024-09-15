<?php

const EOF = false;
const TIMEOUT = true;
const NOT_JSON = null;

// Let's have a global notices-to-errors thingy
set_error_handler(function($severity, $message, $file, $line) {
    if (!(error_reporting() & $severity)) {
        // This error code is not included in error_reporting
        return false;
    }

    // Check if it's an E_NOTICE
    if ($severity === E_NOTICE) {
        // Convert the notice into an ErrorException and throw it
        throw new ProcessingException($message);
    }
});

function getline_timeout($stream, $seconds, $milliseconds)
{
    $inputs = [$stream];
    $write = [];
    $except = [];
    if (stream_select($inputs, $write, $except, $seconds, $milliseconds)) {
        return fgets($stream);
    } else {
        return TIMEOUT;
    }
}

function pipe_period($stream, $period, $line_func, $period_func)
{
    $period_ns = $period * 1000000000;
    $next_tick = hrtime(true) + $period_ns;

    while(true) {
        $left = intval(max($next_tick - hrtime(true), 0) / 1000);
        $left_sec = intdiv($left, 1000000);
        $left_msec = $left - $left_sec * 1000000;

        $line = getline_timeout($stream, $left_sec, $left_msec);
        if ($line === EOF) {
            return;
        } elseif ($line === TIMEOUT) {
            $next_tick += $period_ns;
            $period_func();
        } else {
            try {
                $line_func($line);
            } catch (SkipMessage $e) {
                $e->warn();
            }
        }
    }
}

function journalctl_single($stream, $cmdline_extra, $cursor, $f)
{
    // Convert cursor to cmd
    $after = $cursor === '' ? '' : escapeshellarg('--after-cursor='.$cursor);

    $pipe = popen("exec journalctl -qa --no-tail -o json $after $cmdline_extra", 'r');

    $datafunc = function($line) use ($stream, &$cursor, &$new_data, $f) {
        $json = json_decode($line, true);
        if ($json == NOT_JSON) {
            throw new SkipMessage("Skipping journalctl garbage");
        } else {
            $cursor = $json['__CURSOR'];
            $new_data = true;
            try {
                $ts = bcdiv($json['__REALTIME_TIMESTAMP'], 1000000, 3);
                $output = $f($json, $ts);
                if ($output === null) {
                    throw new SkipMessage("Fallthrough from user function");
                }
                fputcsv($stream, $output);
            } catch (SkipMessage $e) {
                // Add some context
                $e->setCursor($cursor);
                throw $e;
            }
        }
    };
    $cursor_func = function() use ($stream, &$cursor, &$new_data) {
        // Report cursor only if it has changed
        if ($new_data) {
            fputcsv($stream, ['__CURSOR', $cursor]);
            $new_data = false;
        }
    };

    $new_data = false;
    pipe_period($pipe, 5, $datafunc, $cursor_func);

    // After EOF, report last cursor and return it to the caller.
    $cursor_func();
    return $cursor;
}

// Run journal reader twice, first without follow and then with follow
// on. Helps to mitigate certain journald issues when following to
// months-old log files.
function journalctl($stream, $cmdline_extra, $cursor, $f) {
    $second_cursor = journalctl_single($stream, $cmdline_extra, $cursor, $f);
    journalctl_single($stream, '-f '.$cmdline_extra, $second_cursor, $f);
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
        exit($exit_code);
    }
}
