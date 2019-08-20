import { EventBus, ORG_ID, ORGS } from '../bus.api';
import { Logger, LogLevel } from '../log';
import { BusTestUtil } from '../util/test.util';
import { FabricConnectionState } from '../fabric.api';
import { FabricApiImpl } from './fabric';
import { Message } from '../bus';
import { RestService } from '../core/services/rest/rest.service';
import { ServiceLoader } from '../util/service.loader';

/**
 * Copyright(c) VMware Inc. 2019
 */

describe('Fabric Essentials [fabric/fabric.spec]', () => {

    let bus: EventBus;
    let log: Logger;

    beforeEach(
        () => {
            bus = BusTestUtil.bootBusWithOptions(LogLevel.Debug, true);
            bus.api.silenceLog(true);
            bus.api.suppressLog(true);
            bus.api.enableMonitorDump(false);
            bus.enableDevMode();
            log = bus.api.logger();
        }
    );

    it('Check default connected state is false',
        () => {
            expect(bus.fabric.isConnected()).toBeFalsy();
        }
    );

    it('Check connect works',
        (done) => {
            bus.fabric.connect(
                (sessionId: string) => {
                    expect(sessionId).not.toBeNull();
                    done();
                },
                () => {
                }
            );

        }
    );

    it('Check connect works with different values',
        (done) => {
            bus.fabric.connect(
                (sessionId: string) => {
                    expect(sessionId).not.toBeNull();
                    done();
                },
                () => {
                },
                'somehost',
                8080,
                '/somewhere',
                '/jabber',
                '/blabber',
                1,
                true
            );

        }
    );

    it('Check disconnect works',
        (done) => {
            bus.fabric.connect(
                () => {
                    bus.fabric.disconnect();
                },
                () => {
                    done();
                }
            );
        }
    );

    it('Check connect works when called twice',
        (done) => {

            let counter = 0;
            bus.fabric.connect(
                () => {
                    counter++;
                    if (counter > 1) {
                        done();
                    } else {
                        bus.fabric.connect(null, null);
                    }
                },
                () => {
                }
            );

        }
    );

    it('Check org ID can be set.',
        () => {
            bus.fabric.setFabricCurrentOrgId('123-abc');
            expect(bus.stores.getStore(ORGS).get(ORG_ID)).toEqual('123-abc');
        }
    );

    it('Check offline event listeners work',
        (done) => {
            let connectCount = 0;
            bus.fabric.connect(
                () => {
                    connectCount++;
                    const offlineEvent = new Event('offline');
                    bus.api.tickEventLoop(
                        () => {
                            window.dispatchEvent(offlineEvent);
                        }, 20
                    );
                },
                () => {
                    done();
                }
            );
        }
    );

    // throwing strange jasmine error, disabled for now.
    xit('Check online event listeners work',
        (done) => {

            let connectCount = 0;

            bus.fabric.whenConnectionStateChanges()
                .subscribe(
                    (state: FabricConnectionState) => {
                        if (state == FabricConnectionState.Disconnected) {
                            const onlineEvent = new Event('online');
                            window.dispatchEvent(onlineEvent);

                        }
                        if (state == FabricConnectionState.Connected) {
                            if (connectCount <= 1) {
                                bus.fabric.disconnect();
                            }
                        }
                    }
                );

            bus.fabric.connect(
                () => {
                    connectCount++;
                    if (connectCount >= 2) {
                        done();
                    }
                },
                () => {
                }
            );
        }
    );

    it('Check a valid fabric request object can be generated',
        () => {

            const fabricRequest = bus.fabric.generateFabricRequest('testCommand', 'hello');
            expect(fabricRequest.payload).toBe('hello');
            expect(fabricRequest.request).toBe('testCommand');

        }
    );

    it('Check a valid fabric response object can be generated',
        () => {

            const fabricResponse = bus.fabric.generateFabricResponse('123', 'hello');
            expect(fabricResponse.payload).toBe('hello');
            expect(fabricResponse.id).toBe('123');

        }
    );

    it('Check for connection state change to connected',
        (done) => {
            bus.fabric.whenConnectionStateChanges()
                .subscribe(
                    (state: FabricConnectionState) => {
                        expect(state).toEqual(FabricConnectionState.Connected);
                        done();
                    }
                );
            bus.fabric.connect(() => {
            }, () => {
            });
        }
    );

    it('Check fabric version API works.',
        (done) => {

            bus.api.getRequestChannel(FabricApiImpl.versionChannel)
                .subscribe(
                    (msg: Message) => {
                        expect(msg.payload).not.toBeNull();
                        msg.payload.payload = '1.2.3'; // set payload of response, to be 1.2.3
                                                       // , assign response to payload of message.
                        bus.sendResponseMessageWithId(FabricApiImpl.versionChannel, msg.payload, msg.payload.id);
                    }
                );

            bus.fabric.connect(
                () => {
                    bus.fabric.getFabricVersion().subscribe(
                        (id: string) => {
                            expect(id).toEqual('1.2.3');
                            done();
                        }
                    )
                },
                () => {
                }
            );

        }
    );

    it('Check we can switch to fabric REST service.',
        () => {
            spyOn(bus.logger, 'info').and.callThrough();
            bus.fabric.useFabricRestService();
            expect(bus.logger.info)
                .toHaveBeenCalledWith('Switching to Fabric based RestService, all REST calls will be routed via fabric', 'FabricApi');
        }
    );

    it('Check we can switch to local REST service.',
        () => {
            spyOn(bus.logger, 'info').and.callThrough();
            bus.fabric.useLocalRestService();
            expect(bus.logger.info)
                .toHaveBeenCalledWith('Switching local RestService, all REST calls will be routed via browser', 'FabricApi');
        }
    );

    it('Check we can switch to fabric REST service.',
        () => {
            spyOn(bus.logger, 'info').and.callThrough();
            bus.fabric.useFabricRestService();
            expect(bus.logger.info)
                .toHaveBeenCalledWith('Switching to Fabric based RestService, all REST calls will be routed via fabric', 'FabricApi');
        }
    );

    it('Check we can set the session token key',
        () => {
            spyOn(bus.logger, 'debug').and.callThrough();
            bus.fabric.setAccessTokenSessionStorageKey('123');
            expect(bus.logger.debug)
                .toHaveBeenCalledWith('Setting access token session storage key to: 123', 'FabricApi');
        }
    );

    it('Check we cannot get a version from the fabric, if we are not connected',
        (done) => {
            bus.fabric.getFabricVersion().subscribe(
                (value: string) => {
                    expect(value).toEqual('Version unavailable, not connected to fabric');
                    done();
                }
            )
        }
    );

});