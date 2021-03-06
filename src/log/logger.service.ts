/*
 * Copyright 2017-2020 VMware, Inc.
 * SPDX-License-Identifier: BSD-2-Clause
 */

import { LogChannel, LogLevel, LogObject } from './logger.model';
import { GeneralUtil } from '../util/util';

/**
 * This is the low-lever logger that can be instantiated and destroyed at will. Syslog maintains one of these
 * for use across the application, however, anyone can create an instance of this service and manage independent
 * Log Levels and output.
 */
export class Logger {
    public get dateCss() { return this._dateCss; }
    public get fromCss() { return this._fromCss; }
    public get normalCss() { return this._normalCss; }
    public get errorCss() { return this._errorCss; }
    public get warnCss() { return this._warnCss; }
    public get infoCss() { return this._infoCss; }
    public get debugCss() { return this._debugCss; }
    public get verboseCss() { return this._verboseCss; }

    private _dateCss = 'color: blue;';
    private _fromCss = 'color: green';
    private _normalCss = 'color: black;';
    private _errorCss = 'color: red;';
    private _warnCss = 'color: orange;';
    private _infoCss = 'color: brown;';
    private _debugCss = 'color: black;';
    private _verboseCss = 'color: cyan;';

    /* dark theme friendly colors */
    private dateCssDark = 'color: #ec96fb;';
    private fromCssDark = 'color: #FF9800;';
    private normalCssDark = 'color: #03a9f4';
    private errorCssDark = 'color: red;';
    private warnCssDark = 'color: orange;';
    private infoCssDark = 'color: #03a9f4';
    private debugCssDark = 'color: #03a9f4';
    private verboseCssDark = 'color: #03a9f4';

    private _logLevel: LogLevel;
    private _suppress = false;
    private _silent = false;

    private _lastLog: string;

    private _styledLogsSupported: boolean = true;
    private useDarkThemeFriendlyColors: boolean = true;

    setStylingVisble(flag: boolean) {
        this._styledLogsSupported = flag;
    }

    useDarkTheme(flag: boolean) {
        this.useDarkThemeFriendlyColors = flag;
        if (this.useDarkThemeFriendlyColors) {
            this._dateCss = this.dateCssDark;
            this._fromCss = this.fromCssDark;
            this._normalCss = this.normalCssDark;
            this._errorCss = this.errorCssDark;
            this._warnCss = this.warnCssDark;
            this._infoCss = this.infoCssDark;
            this._debugCss = this.debugCssDark;
            this._verboseCss = this.verboseCssDark;
        }
    }

    /**
     * Turn of all logging
     */
    public turnOffAllLogging(): void {
        this.silent(true);
        this.suppress(true);
        this.logLevel = LogLevel.Off;
    }

    /**
     * Turn on all logging
     * Defaults to LogLevel.ERROR; Errors and critical issues only.
     */
    public turnOnAllLogging(): void {
        this.silent(false);
        this.suppress(false);
        this.logLevel = LogLevel.Error;
    }

    /**
     * Turn on VERBOSE logging
     */
    public turnOnVerboseLogging(): void {
        this.turnOnAllLogging();
        this.logLevel = LogLevel.Verbose;
    }

    /**
     * Turn on DEBUG logging
     */
    public turnOnDebugLogging(): void {
        this.turnOnAllLogging();
        this.logLevel = LogLevel.Debug;
    }
    /**
     * Turn on INFO logging
     */
    public turnOnInfoLogging(): void {
        this.turnOnAllLogging();
        this.logLevel = LogLevel.Info;
    }

    /**
     * Turn on WARN logging
     */
    public turnOnWarnLogging(): void {
        this.turnOnAllLogging();
        this.logLevel = LogLevel.Warn;
    }

    /**
     * Turn on ERROR logging
     */
    public turnOnErrorLogging(): void {
        this.turnOnAllLogging();
        this.logLevel = LogLevel.Error;
    }

    /**
     * Returns the last item logged.
     *
     * @returns {string}
     */
    last(): string {
        return this._lastLog;
    }

    /**
     * Clear the last log
     */
    clear() {
        this._lastLog = '';
    }

    /**
     * Sets the minimum level of logging.
     *
     * @param level
     */
    set logLevel(level: LogLevel) {
        this._logLevel = level;
    }

    get logLevel() {
        return this._logLevel;
    }

    get stylingVisble() {
        return this._styledLogsSupported;
    }

    suppress(flag: boolean) {
        this._suppress = flag;
    }

    silent(flag: boolean) {
        this._silent = flag;
    }

    /**
     * Log if the minimum is at or below LogLevel.verbose
     *
     * @param object
     * @param from optional caller filename
     */
    verbose(object: any, from?: string) {
        this.log(new LogObject().build(LogLevel.Verbose, LogChannel.channel, object, from, this._suppress));
    }

    /**
     * Log if the minimum is at or below LogLevel.debug
     *
     * @param object
     * @param from optional caller filename
     */
    debug(object: any, from?: string) {
        this.log(new LogObject().build(LogLevel.Debug, LogChannel.channel, object, from, this._suppress));
    }

    /**
     * Log if the minimum is at or below LogLevel.info
     *
     * @param object
     * @param from optional caller filename
     */
    info(object: any, from?: string) {
        this.log(new LogObject().build(LogLevel.Info, LogChannel.channel, object, from, this._suppress));
    }

    /**
     * Log if the minimum is at or below LogLevel.warn
     *
     * @param object
     * @param from optional caller filename
     */
    warn(object: any, from?: string) {
        this.log(new LogObject().build(LogLevel.Warn, LogChannel.channel, object, from, this._suppress));
    }

    /**
     * Log if the minimum is at or below LogLevel.error
     *
     * @param object
     * @param from optional caller filename
     */
    error(object: any, from?: string) {
        this.log(new LogObject().build(LogLevel.Error, LogChannel.channel, object, from, this._suppress));
    }

    /**
     * Log always
     *
     * @param object
     * @param from optional caller filename
     */
    always(object: any, from?: string) {
        this.log(new LogObject().build(LogLevel.Off, LogChannel.channel, object, from));
    }

    group(logLevel: LogLevel, label: string, suppress = this._suppress) {
        if (logLevel < this.logLevel || suppress) {
            return;
        }
        console.groupCollapsed(label);
    }

    groupEnd(logLevel: LogLevel) {
        if (logLevel < this.logLevel || this._suppress) {
            return;
        }
        console.groupEnd();
    }

    private outputWithOptionalStyle(fn: Function, output: string, severityCss: string) {
        let consoleArgs = [output];
        if (this._styledLogsSupported) {
            consoleArgs = consoleArgs.concat(this.dateCss, this.fromCss, severityCss);
        }
        fn.apply(console, consoleArgs);
    }

    private log(logObject: LogObject) {
        if (logObject.logLevel < this.logLevel) {
            return;
        }
        if (logObject.caller) {
            this._lastLog = '[' + logObject.caller + ']: ' + logObject.object;
        } else {
            this._lastLog = logObject.object;
        }
        if (logObject.suppress) {
            return;
        }

        if (this._silent) {
            return;
        }

        let payloadIsObject = false;
        if (GeneralUtil.isObject(logObject.object)) {
            payloadIsObject = true;
        }

        let date: string = new Date().toLocaleTimeString();
        let output: string = '%c' + logObject.object;
        if (logObject.caller) {
            output += '%c [' + logObject.caller + ']%c';
            output += ' (' + date + ')';
        } else {
            output += '%c%c';
        }

        if (!this._styledLogsSupported) {
            output = output.replace(/%c/g, '');
        }
        

        switch (logObject.logLevel) {
            case LogLevel.Error:
                if (!payloadIsObject) {
                    output = '⁉️ [Error]: ' + output;
                }
                this.outputWithOptionalStyle(console.error, output, this.errorCss);
                break;

            case LogLevel.Warn:
                if (!payloadIsObject) {
                    output = '⚠️ [Warn]: ' + output;
                }
                this.outputWithOptionalStyle(console.warn, output, this.warnCss);
                break;

            case LogLevel.Info:
                if (!payloadIsObject) {
                    output = '▫️️ [Inf]: ' + output;
                }
                this.outputWithOptionalStyle(console.log, output, this.infoCss);
                break;

            case LogLevel.Debug:

                if (!payloadIsObject) {
                    output = '🔸 [Deb]: ' + output;
                }
                this.outputWithOptionalStyle(console.log, output, this.debugCss);
                break;

            case LogLevel.Verbose:
                if (!payloadIsObject) {
                    output = '📍️ [Ver]: ' + output;
                }
                this.outputWithOptionalStyle(console.log, output, this.verboseCss);
                break;

            // default:
            //     this.outputWithOptionalStyle(console.log, output, this.normalCss);
            //     break;
        }
    }
}
