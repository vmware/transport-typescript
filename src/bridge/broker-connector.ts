/*
 * Copyright 2017-2020 VMware, Inc.
 * SPDX-License-Identifier: BSD-2-Clause
 */

import { ConnectionState, StompClient } from './stomp.client';
import { Observable, Subscription } from 'rxjs';
import {
    BrokerConnectorChannel,
    StompBusCommand,
    StompConfig,
    StompMessage,
    StompSession,
    StompSubscription
} from '../bridge/stomp.model';
import { StompParser } from '../bridge/stomp.parser';
import { StompValidator } from './stomp.validator';
import { MonitorChannel, MonitorObject, MonitorType } from '../bus/model/monitor.model';
import { Message } from '../bus/model/message.model';
import { TransportEventBus } from '../bus/bus';
import { ChannelBrokerMapping, EventBus, EventBusEnabled } from '../bus.api';
import { Logger } from '../log';
import { FabricUtil } from '../fabric/fabric.util';
import { FabricConnectionState } from '../fabric.api';
import { GeneralUtil } from '../util/util';

/**
 * Service is responsible for handling all STOMP communications over a socket.
 */

export class BrokerConnector implements EventBusEnabled {

    static serviceName: string = 'stomp.service';
    public reconnectDelay: number = 5000;
    public connectDelay: number = 20;
    public connectingMap: Map<string, boolean> = new Map<string, boolean>();

    private currentSessionMap: Map<string, StompSession> = new Map<string, StompSession>();
    private channelBrokerIdentitiesMap: Map<string, Set<string>> = new Map<string, Set<string>>();

    // helper methods for boilerplate commands.
    static fireSubscriptionCommand(bus: EventBus,
                                   sessionId: string,
                                   destination: string,
                                   subscriptionId: string,
                                   subType: string,
                                   isQueue: boolean,
                                   brokerPrefix: string): void {

        let subscription =
            StompParser.generateStompBrokerSubscriptionRequest(
                sessionId, destination, subscriptionId, isQueue, brokerPrefix
            );

        let command =
            StompParser.generateStompBusCommand(
                subType,
                sessionId,
                destination,
                subscription
            );
        bus.api.send(BrokerConnectorChannel.subscription,
            new Message().request(command), BrokerConnector.serviceName);

    }

    // helper function for subscriptions.
    static fireSubscribeCommand(bus: EventBus,
                                sessionId: string,
                                destination: string,
                                subscriptionId: string,
                                isQueue: boolean,
                                brokerPrefix: string): void {

        BrokerConnector.fireSubscriptionCommand(bus,
            sessionId,
            destination,
            subscriptionId,
            StompClient.STOMP_SUBSCRIBE,
            isQueue,
            brokerPrefix);
    }

    // helper function for unsubscriptions.
    static fireUnSubscribeCommand(bus: EventBus,
                                  sessionId: string,
                                  destination: string,
                                  subscriptionId: string,
                                  isQueue: boolean,
                                  brokerPrefix: string): void {

        BrokerConnector.fireSubscriptionCommand(bus,
            sessionId,
            destination,
            subscriptionId,
            StompClient.STOMP_UNSUBSCRIBE,
            isQueue,
            brokerPrefix);
    }

    // helper function for connecting
    static fireConnectCommand(bus: EventBus, config: StompConfig = null): void {
        let command = StompParser.generateStompBusCommand(StompClient.STOMP_CONNECT, null, null, config);
        bus.api.send(BrokerConnectorChannel.connection,
            new Message().request(command), BrokerConnector.serviceName);
    }

    // helper function for disconnecting
    static fireDisconnectCommand(bus: EventBus, sessionId: string): void {

        let command = StompParser.generateStompBusCommand(StompClient.STOMP_DISCONNECT, sessionId);
        bus.api.send(BrokerConnectorChannel.connection,
            new Message().request(command), BrokerConnector.serviceName);
    }

    constructor(private log: Logger) {

    }

    getName(): string {
        return (this as any).constructor.name;
    }

    private _errorObservables: Map<string, Observable<Error>> = new Map<string, Observable<Error>>();
    private _closeObservables: Map<string, Observable<CloseEvent>> = new Map<string, Observable<CloseEvent>>();
    private _sessions: Map<string, StompSession>;
    private _galacticChannels: Map<string, {connectedBrokers: number}>;
    private _privateChannels: Map<string, {[brokerIdentity: string]: boolean}>;
    private bus: TransportEventBus;
    private reconnectTimerInstance: any;
    private reconnecting: boolean = false;

    setBus(bus: TransportEventBus) {
        this.bus = bus;
    }

    init(bus: TransportEventBus) {

        this.setBus(bus);

        let connectionChannel =
            this.bus.api.getRequestChannel(BrokerConnectorChannel.connection, this.getName());

        let subscriptionChannel =
            this.bus.api.getRequestChannel(BrokerConnectorChannel.subscription, this.getName());

        let monitorChannel =
            this.bus.api.getRequestChannel(MonitorChannel.stream, this.getName());

        connectionChannel.subscribe(
            (msg: Message) => {
                this.processConnectionMessage(msg);
            }
        );

        subscriptionChannel.subscribe(
            (msg: Message) => {
                this.processSubscriptionMessage(msg);
            }
        );

        monitorChannel.subscribe(
            (msg: Message) => {
                this.processMonitorStreamMessage(msg);
            }
        );

        this._sessions = new Map<string, StompSession>();
        this._galacticChannels = new Map<string, {connectedBrokers: number}>();
        this._privateChannels = new Map<string, {[brokerIdentity: string]: boolean}>();

    }

    get galacticChannels(): Map<string, {connectedBrokers: number}> {
        return this._galacticChannels;
    }

    get privateChannels(): Map<string, {[brokerIdentity: string]: boolean}> {
        return this._privateChannels;
    }

    public getSession(id: string): StompSession {
        return this._sessions.get(id);
    }

    get sessions(): Map<string, StompSession> {
        return this._sessions;
    }

    private processMonitorStreamMessage(msg: Message): void {
        if (!StompValidator.validateMonitorMessage(msg)) {
            this.sendBusCommandResponse(StompClient.STOMP_INVALIDMONITOR);
            return;
        }
        let mo = msg.payload as MonitorObject;
        switch (mo.type) {
            case MonitorType.MonitorNewGalacticChannel:
                // if galacticConfig is not present, try detecting the default broker identity with isPrivate
                // set to true for a broadcast channel.
                const galacticConfig: ChannelBrokerMapping = mo.data || {
                    isPrivate: false,
                    brokerIdentity: null
                };
                this.openGalacticChannel(mo.channel, galacticConfig);
                break;

            case MonitorType.MonitorGalacticData:
                const activeSessionIds = Array.from(this._sessions.values()).filter((stompSession: StompSession) => {
                    return this.channelBrokerIdentitiesMap.get(mo.channel).has(GeneralUtil.getFabricConnectionString(
                        stompSession.config.host, stompSession.config.port, stompSession.config.endpoint));
                }).map((stompSession: StompSession) => stompSession.id);

                activeSessionIds.forEach((id: string) => this.sendGalacticMessage(mo.channel, mo.data, id));
                break;

            case MonitorType.MonitorCompleteChannel:
            case MonitorType.MonitorGalacticUnsubscribe:
                if (this._galacticChannels.get(mo.channel)) {
                    this.closeGalacticChannel(mo.channel, mo.data);
                }
                break;
            default:
                break;
        }
    }

    private sendGalacticMessage(channel: string, payload: any, sessionId: string): void {

        // outbound message detected, if it's not a valid remote request, warn the consumer
        if (!FabricUtil.isPayloadFabricRequest(payload)) {
            this.log.warn('Outbound message being sent over WebSocket that has not been correctly ' +
                'wrapped. You may not receive a response, you may receive an error. Please make sure you use ' +
                'fabric.generateFabricRequest() if you\'re not using auto-generated services', this.getName());
        }

        let cleanedChannel = [StompParser.convertChannelToSubscription(channel)];

        this._sessions.forEach(session => {
            const sessionBrokerIdentity = GeneralUtil.getFabricConnectionString(
                session.config.host, session.config.port, session.config.endpoint);

            // send only if broker identity matches
            if (sessionId === session.id) {
                if (session.applicationDestinationPrefix) {
                    cleanedChannel.splice(0, 0, session.applicationDestinationPrefix);
                }

                if (this._privateChannels.get(channel)[sessionBrokerIdentity]) {
                    cleanedChannel.splice(1, 0, 'queue');
                }

                const destination = cleanedChannel.join('/');
                const command: StompBusCommand = StompParser.generateStompBusCommand(
                    StompClient.STOMP_MESSAGE,
                    session.id,
                    destination,
                    StompParser.generateStompReadyMessage(payload, this.getGlobalHeaders())
                );
                this.log.debug('Sending Galactic Message for session ' + session.id +
                    ' to destination ' + destination, this.getName());
                this.sendPacket(command);
            }
        });
    }

    private generateSubscriptionId(sessionId: string, channel: string): string {
        return sessionId + '-' + channel;
    }

    private openGalacticChannel(channel: string, galacticConfig: ChannelBrokerMapping) {
        let cleanedChannel = StompParser.convertChannelToSubscription(channel);

        if (!galacticConfig.brokerIdentity) {
            const defaultBrokerIdentityMatch = this.getDefaultBrokerIdentity();
            if (defaultBrokerIdentityMatch.success) {
                galacticConfig.brokerIdentity = defaultBrokerIdentityMatch.brokerIdentityStr;
            } else {
                if (defaultBrokerIdentityMatch.sessionsNum > 1) {
                    this.log.error('More than one STOMP session was detected when trying to open galactic channel \'' +
                        channel + '\'. You need to explicitly specify the target fabric broker in the second argument ' +
                        'to bus.markChannelAsGalactic()');
                } else if (defaultBrokerIdentityMatch.sessionsNum === 0) {
                    this.log.warn('No session registered');
                }
                return;
            }
        }

        // map channel to broker destination ID (host + port + endpoint)
        if (!this.channelBrokerIdentitiesMap.has(channel)) {
            this.channelBrokerIdentitiesMap.set(channel, new Set<string>([galacticConfig.brokerIdentity]));
        } else {
            this.channelBrokerIdentitiesMap.get(channel).add(galacticConfig.brokerIdentity);
        }

        // if this galactic channel entry doesn't exist, create one and initialize connectedBrokers with 0.
        if (!this._galacticChannels.has(channel)) {
            this._galacticChannels.set(channel, {connectedBrokers: 0});
        }

        // if this private channel entry doesn't exist, create one and initialize it with an empty object.
        if (!this._privateChannels.has(channel)) {
            this._privateChannels.set(channel, {});
        }

        this._privateChannels.get(channel)[galacticConfig.brokerIdentity] = galacticConfig.isPrivate;

        // if we're connected, kick things off
        if (this.isSessionEstablished(galacticConfig)) {
            this._sessions.forEach(session => {
                // subscribe to the broker channel only if broker identity matches
                if (galacticConfig.brokerIdentity ===
                    GeneralUtil.getFabricConnectionString(session.config.host, session.config.port, session.config.endpoint)) {
                    const config = session.config;
                    const subscriptionId = this.generateSubscriptionId(session.id, cleanedChannel);
                    const brokerPrefix = galacticConfig.isPrivate ? config.queueLocation : config.topicLocation;
                    const destination =
                        StompParser.generateGalacticDesintation(brokerPrefix, cleanedChannel);
                    const subscription =
                        StompParser.generateStompBrokerSubscriptionRequest(
                            session.id, destination, subscriptionId, galacticConfig.isPrivate, brokerPrefix
                        );
                    this._galacticChannels.get(channel).connectedBrokers++;
                    this.subscribeToDestination(subscription);
                }

            });
        }
    }

    private closeGalacticChannel(channel: string, galacticConfig?: ChannelBrokerMapping) {

        if (!galacticConfig.brokerIdentity) {
            const defaultBrokerIdentityMatch = this.getDefaultBrokerIdentity();
            if (defaultBrokerIdentityMatch.success) {
                galacticConfig.brokerIdentity = defaultBrokerIdentityMatch.brokerIdentityStr;
            } else {
                if (defaultBrokerIdentityMatch.sessionsNum > 1) {
                    this.log.error('More than one STOMP session was detected when trying to close galactic channel \'' +
                        channel + '\'. You need to explicitly specify the target fabric broker in the second argument ' +
                        'to bus.markChannelAsLocal()');
                } else if (defaultBrokerIdentityMatch.sessionsNum === 0) {
                    this.log.warn('No session registered');
                }
                return;
            }
        }

        let cleanedChannel = StompParser.convertChannelToSubscription(channel);
        if (this._sessions.size >= 1) {
            this._sessions.forEach(session => {
                const config = session.config;
                const sessionBrokerIdentity = GeneralUtil.getFabricConnectionString(
                    config.host, config.port, config.endpoint);

                const isPrivateChannel = this._privateChannels.get(channel)[sessionBrokerIdentity];
                const subscriptionId = this.generateSubscriptionId(session.id, cleanedChannel);
                const brokerPrefix = this._privateChannels.get(channel)[sessionBrokerIdentity] ?
                    config.queueLocation : config.topicLocation;
                const destination =
                    StompParser.generateGalacticDesintation(brokerPrefix, cleanedChannel);
                const subscription = StompParser.generateStompBrokerSubscriptionRequest(
                        session.id, destination, subscriptionId, isPrivateChannel, brokerPrefix);

                // unsubscribe from destination only if broker identity matches
                if (galacticConfig.brokerIdentity === sessionBrokerIdentity) {
                    this.unsubscribeFromDestination(subscription);
                }
            });

            // decrease the connectedBrokers count by 1
            if (this._galacticChannels.get(channel)) {
                this._galacticChannels.get(channel).connectedBrokers--;
            }

            // if connectedBrokers === 0, destroy the entry from galacticChannels map
            if ( this._galacticChannels.get(channel) && this._galacticChannels.get(channel).connectedBrokers === 0) {
                this._galacticChannels.delete(channel);
            }

            // remove the broker identity from the private channel entry
            delete this._privateChannels.get(channel)[galacticConfig.brokerIdentity];

            // remove broker identity from channel-brokerIdentities mapping
            this.channelBrokerIdentitiesMap.get(channel).delete(galacticConfig.brokerIdentity);

            // if there is no key present in the entry, destroy the entry from privateChannels map
            if (Object.keys(this._privateChannels.get(channel)).length === 0) {
                this._privateChannels.delete(channel);
            }
        } else {
            this.log.warn('unable to close galactic channel, no open sessions.', 'BrokerConnector');
        }
    }

    private processSubscriptionMessage(msg: Message): void {
        if (!StompValidator.validateSubscriptionMessage(msg)) {
            this.log.warn('unable to validate inbound subscription message, invalid', 'BrokerConnector');
            return;
        }

        let busCommand = StompParser.extractStompBusCommandFromMessage(msg);
        let sub: StompSubscription = busCommand.payload as StompSubscription;

        // handle subscribe / un-subscribe requests
        switch (busCommand.command) {
            case StompClient.STOMP_SUBSCRIBE:

                this.log.info('subscribing to destination: ' + sub.destination, this.getName());

                // create a subscription payload and throw it on the bus.
                this.subscribeToDestination(sub);
                break;

            case StompClient.STOMP_UNSUBSCRIBE:

                this.log.info('unsubscribing from destination: ' + sub.destination, this.getName());

                // create an unsubscription payload and throw it on the bus.
                this.unsubscribeFromDestination(sub);
                break;

            default:
                break;
        }
    }

    private processConnectionMessage(msg: Message): void {

        if (!StompValidator.validateConnectionMessage(msg)) {
            this.log.warn('unable to validate connection message, invalid command', this.getName());
            return;
        }

        let busCommand = StompParser.extractStompBusCommandFromMessage(msg);

        // handle connect, disconnect requests

        switch (busCommand.command) {

            case StompClient.STOMP_CONNECT:
                let config: StompConfig = busCommand.payload as StompConfig;

                // let everyone know the configuration is good.
                this.sendBusCommandResponse(StompClient.STOMP_CONFIGURED);

                // connect, or re-use existing socket.
                this.bus.api.tickEventLoop(
                    () => {
                        this.connectClient(config);
                    }, this.connectDelay
                );
                break;

            case StompClient.STOMP_DISCONNECT:
                // let everyone know we're disconnecting
                this.sendBusCommandResponse(StompClient.STOMP_DISCONNECTING);

                // disconnect
                this.disconnectClient(busCommand.session);
                break;

            default:
                break;
        }
    }

    private sendBusCommandResponseRaw(command: StompBusCommand,
                                      channel: string = BrokerConnectorChannel.status,
                                      echoStatus: boolean = false,
                                      error: boolean = false): void {

        let messageType: Message =
            new Message(command.payload).response(command);
        if (error) {
            messageType =
                new Message(command.payload).error(command);
        }
        this.bus.api.send(
            channel,
            messageType,
            this.getName()
        );

        if (echoStatus) {
            this.bus.api.send(
                BrokerConnectorChannel.status,
                messageType,
                this.getName()
            );
        }
    }

    private sendBusCommandResponse(status: string,
                                   channel: string = BrokerConnectorChannel.status,
                                   echoStatus: boolean = false): void {
        let msg: StompBusCommand = StompParser.generateStompBusCommand(status);
        this.sendBusCommandResponseRaw(msg, channel, echoStatus);
    }

    private getGlobalHeaders(): {[key: string]: string | number} {
        const accessTokenHeaderKey = this.bus.fabric.accessTokenHeaderKey;
        const headers: any = {
            [accessTokenHeaderKey]: this.bus.fabric.getAccessToken(),
        };

        return headers;
    }

    public connectClient(config: StompConfig): void {

        const connString: string = GeneralUtil.getFabricConnectionString(config.host, config.port, config.endpoint);

        // Ignore the connect request in case we are in connecting or connected state.
        if (this.currentSessionMap.has(connString) && this.currentSessionMap.get(connString).client &&
              (this.currentSessionMap.get(connString).client.connectionState === ConnectionState.Connecting ||
                  this.currentSessionMap.get(connString).client.connectionState === ConnectionState.Connected)) {
            return;
        }

        let session = new StompSession(config, this.log, this.bus);
        this.currentSessionMap.set(connString, session);

        let connection = session.connect(this.getGlobalHeaders());
        this.connectingMap.set(connString, true);

        config.connectionSubjectRef = connection;

        connection.subscribe(
            () => {
                clearInterval(this.reconnectTimerInstance);
                this.reconnecting = false;
                //this.connecting = false;

                session.connectionCount++;

                if (session.connectionCount < session.config.brokerConnectCount) {
                    this.log.info('Connection message ' +
                        session.connectionCount + ' of ' + config.brokerConnectCount + ' received', this.getName());
                }

                // this checks to see if the number of brokers configured send by the bus connectBroker() method
                // has been hit yet for this session. The broker connect count is equivalent to how many CONNECTED
                // events will be sent down the socket.
                if (session.config.brokerConnectCount === session.connectionCount) {

                    // Making sure any duplicate runs (just in case things go mad), only run once per session.
                    if (!session.connected) {

                        let message: StompBusCommand =
                            StompParser.generateStompBusCommand(
                                StompClient.STOMP_CONNECTED,    // not a command, but used for local notifications.
                                session.id,                      // each broker requires a session.
                                undefined,
                                GeneralUtil.getFabricConnectionString(
                                    session.config.host, session.config.port, session.config.endpoint)
                            );

                        this.sendBusCommandResponseRaw(message, BrokerConnectorChannel.connection, true);
                        this.bus.api.getMonitorStream().send(
                            new Message().response(
                                new MonitorObject()
                                    .build(MonitorType.MonitorBrokerConnectorConnected,
                                        BrokerConnectorChannel.connection, this.getName(), '')
                            )
                        );

                        // these are now available;
                        this._errorObservables.set(session.id, session.client.socketErrorObserver);
                        this._closeObservables.set(session.id, session.client.socketCloseObserver);

                        // add session to map
                        this._sessions.set(session.id, session);
                        this.subscribeToClientObservables(session);

                        // subscribe to all open channels
                        this._galacticChannels.forEach((open, channel) => {
                            const sessionBrokerIdentity = GeneralUtil.getFabricConnectionString(
                                session.config.host, session.config.port, session.config.endpoint);

                            // only open galactic channels whose broker identity matches that of the session config
                            if (this.channelBrokerIdentitiesMap.get(channel).has(sessionBrokerIdentity)) {
                                // if the channel is open
                                this.openGalacticChannel(
                                    channel,
                                    {
                                        isPrivate: this._privateChannels.get(channel)[sessionBrokerIdentity],
                                        brokerIdentity: connString
                                    });
                            }
                        });
                        session.connected = true;
                    }
                } else if (session.config.brokerConnectCount > session.connectionCount) {
                    // more connected messages than expected, ignore, but tell the bus duplicates came in beyond
                    // what we were expecting.
                    let message: StompBusCommand =
                        StompParser.generateStompBusCommand(
                            StompClient.STOMP_CONNECTED_DUPLICATE, // Spring broker sends double CONNECT on
                            session.id                              // each broker requires a session.
                        );

                    this.sendBusCommandResponseRaw(message, BrokerConnectorChannel.connection, true);
                }

                // upon (re)connection update FabricConnectionState store
                this.bus.fabric.getConnectionStateStore(connString)
                    .put(connString, FabricConnectionState.Connected, FabricConnectionState.Connected);
            }
        );
    }

    public disconnectClient(sessionId: string): void {

        let session = this._sessions.get(sessionId);
        if (session && session !== null) {
            this.clearGalacticSubscriptionsFromSession(session);
            session.disconnect(this.getGlobalHeaders());
            this._sessions.delete(sessionId);
        } else {
            this.log.warn('unable to disconnect client, no active session with id: ' +
                sessionId, 'BrokerConnector');
        }
    }

    public sendPacket(data: StompBusCommand): void {
        let session = this._sessions.get(data.session);
        if (session && session !== null) {
            let message: StompMessage = data.payload as StompMessage;

            // wire in the local broker session id.
            if (!message.headers['session']) {
                this.log.debug('sendPacket(): adding local broker session ID to message: '
                    + data.session, this.getName());

                message.headers.session = data.session;
            } else {
                this.log.debug('sendPacket(): message headers already contain sessionId', this.getName());
            }
            session.send(data.destination, message.headers, message.body);
        } else {
            this.log.warn('unable to send packet, session is empty', this.getName());
        }
    }

    private clearGalacticSubscriptionsFromSession(session: StompSession) {
        if (!session || !session.getGalacticSubscriptions()) {
            return;
        }
        session.getGalacticSubscriptions().forEach((sub: Subscription, channelName: string) => {
           if (sub) {
              sub.unsubscribe();
              // close the channel to fix the channel reference count
              if (this._galacticChannels.get(channelName).connectedBrokers === 0) {
                  this.bus.api.close(channelName, this.getName());
              }
           }
        });
        session.getGalacticSubscriptions().clear();
    }

    private reconnectTimer(config: StompConfig) {

        // only enable if autoReconnect is set.
        if (config.autoReconnect) {
            const connect = () => {
                this.log.warn('Trying to reconnect to broker....', this.getName());
                this.bus.api.tickEventLoop(
                    () => {
                        this.connectClient(config);
                    }, this.connectDelay
                );
            };
            this.reconnectTimerInstance = setInterval(connect, this.reconnectDelay);
        }
    }

    private subscribeToClientObservables(s: StompSession): void {

        this._closeObservables.get(s.id).subscribe(
            (evt: any) => {
                const sessionsToDestroy: string[] = [];
                this.log.warn('WebSocket to broker closed!', this.getName());
                this.sessions.forEach(session => {
                    if (session.config.host === (evt.config as StompConfig).host &&
                        session.config.port === (evt.config as StompConfig).port) {
                        sessionsToDestroy.push(session.id);
                    }
                });

                sessionsToDestroy.forEach((sessionId: string) => {
                    const session = this.sessions.get(sessionId);
                    for (let [chan, bIds] of this.channelBrokerIdentitiesMap.entries()) {
                        // decrement connectedBrokers for the channel
                        // NOTE: in an unlikely scenario, there could be a "double free" condition of the galactic channel object
                        // that had been already cleaned up by this.closeGalacticChannel() when an event from the _closeObservable
                        // tried to access the object and decrement the connectedBrokers counter, leading to access to undefined.
                        // this is an extremely hard to reproduce issue, but I suspect that it greatly has to do with the browser
                        // tab process being put to sleep (after a long idle) and a resultant race condition that may have led to
                        // the above chain of actions. a fix implemented here is to protect the decrement instruction against
                        // a situation where the galactic channel object has been destroyed.
                        if (this._galacticChannels.has(chan)) {
                            this._galacticChannels.get(chan).connectedBrokers--;
                        }

                        // If the client will not immediately try to reconnect, clean up by
                        // removing the connection string from the channelBrokerIdentitiesMap
                        if (!evt.config.autoReconnect) {
                            bIds.delete(GeneralUtil.getFabricConnectionString(session.config.host, session.config.port, session.config.endpoint));
                        }
                    }
                    this.clearGalacticSubscriptionsFromSession(session);
                    this.sessions.delete(sessionId);
                });

                if (!this.reconnecting) {
                    this.log.info('Starting re-connection timer', this.getName());
                    this.reconnecting = true;
                    this.reconnectTimer(evt.config);
                }
                this.sendBusCommandResponse(
                    StompClient.STOMP_DISCONNECTED,
                    BrokerConnectorChannel.connection, true);
            }
        );

        this._errorObservables.get(s.id).subscribe(
            (err: any) => {
                this.log.error('Error occurred with WebSocket, Unable to connect to broker!', this.getName());
                this.sessions.forEach(
                    session => this.clearGalacticSubscriptionsFromSession(session));
                this._sessions.clear();
                let msg
                    = StompParser.generateStompBusCommand(
                    StompClient.STOMP_ERROR, '', '', err);

                this.sendBusCommandResponseRaw(msg, BrokerConnectorChannel.error, true, true);
            }
        );
    }

    public subscribeToDestination(data: StompSubscription): void {
        let session = this._sessions.get(data.session);
        if (session && session !== null) {
            // subscribe to broker destination
            let subscriptionSubject = session.subscribe(data.destination, data.id, this.getGlobalHeaders());
            let message: StompBusCommand =
                StompParser.generateStompBusCommand(
                    StompClient.STOMP_SUBSCRIBED,
                    session.id,
                    data.destination
                );

            const channel: string = StompParser.convertTopicOrQueueToChannel(
                data.destination, data.brokerPrefix);

            const chan: Observable<Message> =
                this.bus.api.getRequestChannel(channel, this.getName());

            const sub: Subscription = chan.subscribe(
                (msg: Message) => {

                    // send galactic message.
                    this.sendGalacticMessage(channel, msg.payload, session.id);
                }
            );
            session.addGalacticSubscription(channel, sub);

            // let the bus know.
            this.sendBusCommandResponseRaw(message, BrokerConnectorChannel.subscription, true);

            let subjectSubscription = subscriptionSubject.subscribe(
                (msg: StompMessage) => {

                    let respChan =
                        StompParser.convertSubscriptionToChannel(
                            data.destination,
                            data.isQueue ? session.config.queueLocation : session.config.topicLocation);
                    let payload = JSON.parse(msg.body);

                    const respChannelObject = this.bus.api.getChannelObject(respChan);

                    // not sure if this has value. leaving out for now.
                    // inbound message detected, if it's not a valid remote response, warn the consumer
                    if (!FabricUtil.isPayloadFabricResponse(payload)) {
                        // this.log.warn('Inbound message being sent via WebSocket has not been correctly ' +
                        //     'wrapped. Response data may be incorrectly packaged and may cause run-time error.');
                    }

                    // check if response is an error
                    if (payload && payload.error) {
                        // send an error instead, this is not a good message.
                        // a STOMP message needs to be ALWAYS sent back to the original request that it's intended for.
                        // for that reason, we are setting the Message ID with the payload ID.
                        respChannelObject.stream.next(new Message(payload.id).error(payload));

                    } else {
                        // bypass event loop for fast incoming socket events, the loop will slow things down.
                        respChannelObject.stream.next(new Message(payload.id).response(payload));
                    }
                }
            );

            this._closeObservables.get(session.id).subscribe(
                () => {
                    subjectSubscription.unsubscribe();
                }
            );

        } else {
            let msg
                = StompParser.generateStompBusCommand(
                StompClient.STOMP_ERROR, '', '', 'cannot subscribe, session does not exist.');

            this.sendBusCommandResponseRaw(msg, BrokerConnectorChannel.error, true, true);
        }
    }

    public unsubscribeFromDestination(data: StompSubscription): void {
        let session = this._sessions.get(data.session);
        if (session && session !== null) {
            session.unsubscribe(data.id, this.getGlobalHeaders());

            let message: StompBusCommand =
                StompParser.generateStompBusCommand(
                    StompClient.STOMP_UNSUBSCRIBED,
                    session.id,
                    data.destination
                );

            // let the bus know.
            this.sendBusCommandResponseRaw(message, BrokerConnectorChannel.subscription, true);
            const channel: string = StompParser.convertTopicOrQueueToChannel(data.destination, data.brokerPrefix);

            const sub: Subscription = session.getGalacticSubscription(channel);

            if (sub) {
                sub.unsubscribe();
                session.removeGalacticSubscription(channel);
                this.bus.api.close(channel, this.getName());
            } else {
                this.log.warn('unable to unsubscribe, no galactic subscription found for id: '
                    + data.session, this.getName());

                this.bus.api.close(channel, this.getName());
            }
        } else {
            this.log.warn('unable to unsubscribe, no session found for id: ' + data.session, this.getName());
        }
    }

    /**
     * Return true if session is established with the broker identified in galacticConfig
     * @param {ChannelBrokerMapping} galacticConfig
     * @return {boolean} true if session is established with the broker
     */
    private isSessionEstablished(galacticConfig: ChannelBrokerMapping): boolean {
        return Array.from(this._sessions.values()).some((stompSession: StompSession) =>
            GeneralUtil.getFabricConnectionString(
                stompSession.config.host,
                stompSession.config.port,
                stompSession.config.endpoint) === galacticConfig.brokerIdentity);
    }

    /**
     * Return default brokerIdentity.
     * @return {string} broker identity if there is only one session or
     *         null if there more than one STOMP session is present
     */
    private getDefaultBrokerIdentity(): {success: boolean; sessionsNum: number; brokerIdentityStr: string} {
        if (this._sessions.size > 1) {
            return {success: false, sessionsNum: this._sessions.size, brokerIdentityStr: null};
        }

        const firstSession = Array.from(this._sessions.values())[0];
        if (!firstSession) {
            return {success: false, sessionsNum: 0, brokerIdentityStr: null};
        }

        return {success: true, sessionsNum: 1, brokerIdentityStr: GeneralUtil.getFabricConnectionString(
            firstSession.config.host, firstSession.config.port, firstSession.config.endpoint)};
    }
}


