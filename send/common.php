<?php

const EOF = false;
const TIMEOUT = true;
const NOT_JSON = null;

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
        $left = intdiv(max($next_tick - hrtime(true), 0), 1000);
        $left_sec = intdiv($left, 1000000);
        $left_msec = $left - $left_sec * 1000000;

        $line = getline_timeout($stream, $left_sec, $left_msec);
        if ($line === EOF) {
            return;
        } elseif ($line === TIMEOUT) {
            $next_tick += $period_ns;
            $period_func();
        } else {
            $line_func($line);
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
            fprintf(STDERR, "Skipping journalctl garbage\n");
        } else {
            $cursor = $json['__CURSOR'];
            $new_data = true;
            $output = $f($json);
            if (is_array($output)) {
                fputcsv($stream, $output);
            } else {
                fprintf(STDERR, "User function skipped line\n");
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
    pipe_period($pipe, 10, $datafunc, $cursor_func);

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
