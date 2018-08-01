/**
 * Copyright(c) VMware Inc. 2018
 */
import {
    MessageProxyConfig,
    IFrameProxyControl,
    ProxyType,
    BusProxyMessage,
    ProxyControl,
    ProxyControlType, ProxyControlPayload, ProxyState
} from './message.proxy';
import { LogLevel } from '../log/logger.model';
import { LogUtil } from '../log/util';
import { ChannelName, EventBus, EventBusEnabled, MessageType } from '../bus.api';
import { Observable } from 'rxjs';
import { MonitorChannel, MonitorObject, MonitorType } from '../bus/model/monitor.model';
import { Subscription } from 'rxjs/internal/Subscription';
import { Message } from '../bus/model/message.model';
import { GeneralUtil } from '../util/util';

const domWindow: any = window;

export class ProxyControlImpl implements IFrameProxyControl, EventBusEnabled {

    private readonly proxyControlChannel: string = '__proxycontrol__';

    getName(): string {
        return `ProxyControl-${EventBus.id}`;
    }

    /**
     * Handle inbound postMessage events.
     */
    private postMessageEventHandlerBinding: EventListenerObject;


    /**
     * Track is proxy is operating.
     * @type {boolean}
     */
    private enabled: boolean = true;

    /**
     * Definition of the target origin(s) to be acceptable, for security purposes.
     */
    private targetOrigin: string[];

    /**
     * Target all frames? The proxy will broadcast to everyone listening. Defaults to true
     * @type {boolean}
     */
    private targetAllFramesValue: boolean = true;

    /**
     * Target specific frames only. The proxy will only broadcast to the defined frames
     */
    private targetedFrames: string[];

    /**
     * Only authorized channels will be rebroadcast and handled, prevents unwanted payloads reaching private
     * internal channels, not intended to be exposed.
     */
    private authorizedChannels: ChannelName[];

    private monitorChannel: Observable<Message>;
    private monitorSubscription: Subscription;

    private knownBusInstances: Map<string, ProxyState>;


    /**
     * Type of proxy operating.
     */
    private proxyType: ProxyType;

    private listening: boolean = false;

    private parentOriginValue: string;

    /**
     * Default proxy type is parent.
     * @param {ProxyType} proxyType
     */
    constructor(private bus: EventBus, private config: MessageProxyConfig) {
        // do something

        this.targetOrigin = ['*']; // default, which is wide open, so this should be set!
        if (config) {
            if (config.acceptedOrigins) {
                this.targetOrigin = config.acceptedOrigins;
            }

            // pull out properties from config,
            if (!config.targetAllFrames) {
                this.targetAllFrames(false);
            }

            if (config.targetSpecificFrames && config.targetSpecificFrames.length > 0) {
                this.targetedFrames = config.targetSpecificFrames;
            } else {
                this.targetedFrames = []; // don't want it to be empty.
            }

            if (config.protectedChannels && config.protectedChannels.length > 0) {
                this.authorizedChannels = config.protectedChannels;
            } else {
                this.authorizedChannels = []; // don't want this empty either.
            }

            if (config.parentOrigin) {
                this.parentOriginValue = config.parentOrigin;
            } else {
                this.parentOriginValue = '*';
            }

            // configure listening type.
            if (config.proxyType) {
                this.proxyType = config.proxyType;
            }
        } else {
            this.bus.logger.error(
                'Message Proxy cannot start. No configuration has been set.', this.getName());
            return;
        }

        this.authorizedChannels.push(this.proxyControlChannel);

        // connect to monitor;
        this.monitorChannel = this.bus.api.getChannel(MonitorChannel.stream);

        // create bus instance map
        this.knownBusInstances = new Map<string, ProxyState>();

        // start listening by default
        this.listen();

    }

    listen(): void {
        if (this.enabled && !this.listening) {
            this.listening = true;
            this.postMessageEventHandlerBinding = this.parentEventHandler.bind(this);

            switch (this.config.proxyType) {
                case ProxyType.Parent:
                    this.listenForInboundMessageEvents();
                    this.relayMessagesToChildren();
                    break;

                case ProxyType.Child:
                    this.listenForInboundMessageEvents();
                    this.registerChildBusWithParent();
                    this.relayMessagesToParent();
                    break;

                case ProxyType.Hybrid:
                    // TODO: Build clean chaining mechanism for handling nested operations.
                    break;

                default:
                    break;
            }
        }
    }

    private registerChildBusWithParent(): void {

        const proxyCommand: ProxyControlPayload = {
            command: ProxyControlType.RegisterEventBus,
            body: EventBus.id,
            proxyType: this.proxyType
        };
        this.sendControlToParent(proxyCommand);

    }

    private listenForInboundMessageEvents(): void {
        domWindow.addEventListener('message', this.postMessageEventHandlerBinding, {capture: true});
    }

    private relayMessagesToChildren(): void {

        // use the low level bus API's for this work.
        this.monitorSubscription = this.monitorChannel.subscribe(
            (message: Message) => {

                let mo = message.payload as MonitorObject;
                switch (mo.type) {
                    case MonitorType.MonitorData:

                        // is this for an authorized channel?
                        let auth = false;
                        for (let chan of this.authorizedChannels) {

                            if (mo.channel === chan) {
                                this.sendMessageToChildFrames(mo.data, chan);
                            }
                        }
                        break;
                    default:
                        break;
                }
            }
        );
    }

    private relayMessagesToParent(): void {

        // use the low level bus API's for this work.
        this.monitorSubscription = this.monitorChannel.subscribe(
            (message: Message) => {

                let mo = message.payload as MonitorObject;
                switch (mo.type) {
                    case MonitorType.MonitorData:

                        // is this for an authorized channel?
                        for (let chan of this.authorizedChannels) {
                            if (mo.channel === chan && !mo.data.proxyRebroadcast) {
                                this.sendMessageToParent(mo.data, chan);
                            }
                        }
                        break;
                    default:
                        break;
                }
            }
        );

    }

    private sendMessageToParent(message: Message, chan: ChannelName): void {
        this.bus.logger.debug(
            `Authorized message received on: [${chan}], sending to parent frame`, this.getName());

        const proxyMessage: BusProxyMessage = new BusProxyMessage(message.payload, chan, message.type,  EventBus.id);
        domWindow.parent.postMessage(proxyMessage, this.parentOriginValue);

    }

    private sendControlToParent(control: ProxyControlPayload): void {
        this.bus.logger.debug(
            `Authorized control message received on: [${this.proxyControlChannel}], sending to parent frame`,
            this.getName());

        const proxyMessage: BusProxyMessage =
            new BusProxyMessage(control, this.proxyControlChannel, MessageType.MessageTypeControl, EventBus.id);
        proxyMessage.control = control.command;

        domWindow.parent.postMessage(proxyMessage, this.parentOriginValue);

    }

    private sendMessageToChildFrames(message: Message, chan: ChannelName): void {
        this.bus.logger.debug(
            `Authorized message received on: [${chan}], sending to child frames`, this.getName());


        const proxyMessage: BusProxyMessage = new BusProxyMessage(message.payload, chan, message.type, message.id);

        // if targeting all frames, extract all frames on the page and post messages to them.
        if (this.targetAllFramesValue) {
            const frames: any = domWindow.frames;
            const frameCount = domWindow.frames.length;
            if (frameCount > 0) {
                for (let i = 0; i < frameCount; i++) {
                    frames[i].postMessage(proxyMessage, this.parentOriginValue);
                }
            }
        }

        if (this.targetedFrames.length > 0) {
            for (let frameId of this.targetedFrames) {
                const frame = domWindow.document.getElementById(frameId).contentWindow;
                if (frame) {
                    frame.postMessage(proxyMessage, this.parentOriginValue);
                }
            }
        }
    }


    private parentEventHandler(event: MessageEvent): void {

        // drop the message if it originated from this bus, otherwise we will see duplicates.
        if (event.data && event.data.from) {
            if (event.data.from === `proxy-${EventBus.id}`) {
                return;
            }
        }

        // check origin
        let originOk;
        for (let origin of this.targetOrigin) {
            originOk = origin === '*'; // if this is in place, we don't care.
            if (!originOk && origin === event.origin) {
                originOk = true;
            }
        }

        if (!originOk) {
            this.bus.logger.warn(
                'Message refused, origin not registered: ' + event.origin, this.getName());
            return;
        }

        // check if the message contains a payload, and check if it is a serialized bus message.
        if (event.data && event.data !== '') {
            const data: any = event.data;
            if (data.hasOwnProperty('channel') && data.hasOwnProperty('type') && data.hasOwnProperty('payload')) {

                // validate proxy message
                if (data.channel === null || data.channel === '') {
                    this.bus.logger.warn(
                        'Proxy Message invalid - ignored. No channel supplied', this.getName());
                    return;
                }
                if (data.type === null || data.type === '') {
                    this.bus.logger.warn(
                        'Proxy Message invalid - ignored. No message type supplied', this.getName());
                    return;
                }
                if (data.payload === null || data.payload === '') {
                    this.bus.logger.warn(
                        'Proxy Message invalid - ignored. Payload is empty', this.getName());
                    return;
                }

                // looks like the message is valid, lets check the channel for authorization.
                if (!this.validateChannel(data.channel)) {
                    this.bus.logger.warn(
                        'Proxy Message valid, but channel is not authorized: [' + data.channel + ']', this.getName());
                    return;
                } else {

                    // everything checks out!
                    // determine if this event is a control event or a regular message to proxy
                    if (data.control != null) {
                        const payload: ProxyControlPayload = data.payload;
                        let state: ProxyState;
                        switch (data.control) {

                            // register bus.
                            case ProxyControlType.RegisterEventBus:
                                this.knownBusInstances.set(payload.body, {type: payload.proxyType, active: true});
                                this.bus.logger.info(
                                    `Child Event Bus Registered: ${payload.body}`, this.getName());

                                break;

                            // set instance to active.
                            case ProxyControlType.BusStartListening:
                                state = this.knownBusInstances.get(payload.body);
                                if (state) {
                                    state.active = true;
                                }
                                this.knownBusInstances.set(payload.body, state);
                                break;

                            // set instance to inactive.
                            case ProxyControlType.BusStopListening:
                                state = this.knownBusInstances.get(payload.body);
                                if (state) {
                                    state.active = false;
                                }
                                this.knownBusInstances.set(payload.body, state);
                                break;

                            default:
                                break;
                        }

                    } else {
                        this.proxyMessage(data, event.origin);
                    }
                }

            } else {
                this.bus.logger.debug(
                    'Message Ignored, not intended for the bus.', this.getName());
                this.bus.logger.group(LogLevel.Info, '📦 Message Payload (Ignored)');
                this.bus.logger.debug(LogUtil.pretty(data));
                this.bus.logger.groupEnd(LogLevel.Info);
                return;
            }
        } else {

            this.bus.logger.debug(
                'Message Ignored, it contains no payload', this.getName());
            return;

        }
    }

    private validateChannel(requestedChannel: ChannelName): boolean {
        return this.authorizedChannels.includes(requestedChannel);
    }

    private proxyMessage(message: BusProxyMessage, origin: string): void {
        let msg: Message;
        switch (message.type) {
            case MessageType.MessageTypeRequest:

                // build a message manually and set the proxy rebroadcast flag.
                msg = new Message(`proxy-${message.from}`).request(message.payload);
                msg.proxyRebroadcast = true; // this will prevent the messge from being re-picked up by the proxy.
                this.bus.api.send(message.channel, msg, this.getName() + '-' + origin);
                break;

            case MessageType.MessageTypeResponse:

                msg = new Message(`proxy-${message.from}`).response(message.payload);
                msg.proxyRebroadcast = true; // this will prevent the messge from being re-picked up by the proxy.
                this.bus.api.send(message.channel, msg, this.getName() + '-' + origin);
                break;

            case MessageType.MessageTypeError:
                msg = new Message(`proxy-${message.from}`).error(message.payload);
                msg.proxyRebroadcast = true; // this will prevent the messge from being re-picked up by the proxy.
                this.bus.api.send(message.channel, msg, this.getName() + '-' + origin);
                break;

        }
    }

    listeningAs(): ProxyType {
        return this.proxyType;
    }


    stopListening(): void {
        if (this.listening) {
            this.listening = false;
            this.monitorSubscription.unsubscribe();
            domWindow.removeEventListener('message', this.postMessageEventHandlerBinding);
            const control: ProxyControlPayload = {
                proxyType: this.proxyType,
                command: ProxyControlType.BusStopListening,
                body: EventBus.id
            };
            this.sendControlToParent(control);
        }
    }

    targetAllFrames(allFrames: boolean): void {

        // can only be applied if there are no target frames registered;
        if (!this.targetedFrames || this.targetedFrames.length <= 0) {
            this.targetAllFramesValue = allFrames;
        }
    }

    isTargetingAllFrames(): boolean {
        return this.targetAllFramesValue;
    }

    addAllowedTargetOrigin(origin: string): void {
        if (this.targetOrigin.indexOf(origin) < 0) {
            this.targetOrigin.push(origin);
        }
    }

    addTargetedFrame(frameId: string): void {
        if (this.targetedFrames.indexOf(frameId) < 0) {
            this.targetedFrames.push(frameId);
        }
    }

    addAuthorizedChannel(channel: ChannelName): void {
        if (this.authorizedChannels.indexOf(channel) < 0) {
            this.authorizedChannels.push(channel);
        }
    }

    removeAllowedTargetOrigin(origin: string): void {
        const index = this.targetOrigin.indexOf(origin);
        if (index >= 0) {
            this.targetOrigin.splice(index, 1);
        }
    }

    removeAuthorizedChannel(channel: ChannelName): void {
        const index = this.authorizedChannels.indexOf(channel);
        if (index >= 0) {
            this.authorizedChannels.splice(index, 1);
        }
    }

    removeTargetedFrame(frameId: string): void {
        const index = this.targetedFrames.indexOf(frameId);
        if (index >= 0) {
            this.targetedFrames.splice(index, 1);
        }
    }

    getAuthorizedChannels(): ChannelName[] {
        return this.authorizedChannels;
    }

    getAllowedOrigins(): string[] {
        return this.targetOrigin;
    }

    getTargetedFrames(): string[] {
        return this.targetedFrames;
    }

    isListening(): boolean {
        return this.listening;
    }

    getParentOrigin(): string {
        return this.parentOriginValue;
    }

    setParentOrigin(origin: string): void {
        this.parentOriginValue = origin;
    }

    getKnownBusInstances(): Map<string, ProxyState> {
        return new Map(this.knownBusInstances.entries());
    }
}