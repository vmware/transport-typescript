import { Logger, LogLevel } from '../../../log';
import { EventBus } from '../../../bus.api';
import { BusTestUtil } from '../../../util/test.util';
import { ServiceLoader } from '../../../util/service.loader';
import { FakeService } from '../mocks/fake.service.mock';
import { RestService } from './rest.service';
import { MockHttpClient } from './httpclient.mock';
import {
    FakeApiCallRequestObject, FakeApiCallResponseObject,
    FakeChannel, FakeGenericRequestObject,
    FakeGetServiceVersionRequestObject,
    FakeGetServiceVersionResponseObject,
    FakeRequest,
    FakeRestRelayRequestObject,
    FakeRestRelayResponseObject
} from '../mocks/service.objects.mock';
import { APIRequest, Message } from '../../../bus';
import { GeneralUtil } from '../../../util/util';
import { ServiceVersion } from '../../abstractions/service.version';

// These test the RestService and the autogeneration abstractions using FakeService

describe('Fake Service [services/rest/rest.service.spec]', () => {

    let bus: EventBus;
    let log: Logger;
    let httpClient: any;
    const restPayloadGet: any = {
        op: 'GET',
        uri: 'http://some/uri',
        headers: { seven: 'eight' }
    };
    const restPayloadPost: any = {
        op: 'POST',
        uri: 'http://some/uri',
        body: { five: 'six' },
        headers: { seven: 'eight' }
    };

    beforeEach(
        () => {
            bus = BusTestUtil.bootBusWithOptions(LogLevel.Debug, true);
            bus.api.silenceLog(true);
            bus.api.suppressLog(true);
            bus.api.enableMonitorDump(false);
            log = bus.api.logger();

            httpClient = new MockHttpClient();
            httpClient.mustFail = false;
            httpClient.errCode = 200;

            ServiceLoader.addService(RestService, httpClient);
            ServiceLoader.addService(FakeService);

        }
    );

    afterEach(
        () => {
            ServiceLoader.destroyAllServices();
        }
    );

    it('Should have loaded the Fake service mock and RestService with the mock httpClient.',
        () => {
            let services = ServiceLoader.getLoadedServices();
            expect(services.size).toEqual(2);
        }
    );

    it('Should get version from FakeService.',
        () => {
            let channel = bus.api.getResponseChannel(FakeChannel.request);
            const id = GeneralUtil.genUUIDShort();

            expect(channel)
                .not
                .toBeUndefined();
            channel.subscribe(
                (message: Message) => {
                    expect(message.id).toEqual(id);
                    const responseObject = message.payload as FakeGetServiceVersionResponseObject;
                    const serviceVersion = responseObject.payload as ServiceVersion;
                    expect(serviceVersion.isValid).toBeTruthy();
                    expect(serviceVersion.name).toBe('FakeService');
                    expect(serviceVersion.version).toBe('1');
                }
            );

            const requestObject = new FakeGetServiceVersionRequestObject(FakeChannel.request);
            bus.sendRequestMessageWithId(FakeChannel.request, requestObject, id);
        }
    );

    it('Should fail on a bad request to FakeService (negative test).',
        () => {
            let channel = bus.api.getResponseChannel(FakeChannel.request);
            const id = GeneralUtil.genUUIDShort();


            expect(channel)
                .not
                .toBeUndefined();
            channel.subscribe(
                (message: Message) => {
                    expect(message.id).toEqual(id);
                    expect(message.isError()).toBeTruthy();
                }
            );

            const requestObject = new FakeGenericRequestObject(FakeChannel.request, FakeRequest.BadRequest);
            bus.sendRequestMessageWithId(FakeChannel.request, requestObject, id);
        }
    );

    it('Should simulate an autogenerated API call.',
        () => {
            let channel = bus.api.getResponseChannel(FakeChannel.request);
            const id = GeneralUtil.genUUIDShort();

            expect(channel)
                .not
                .toBeUndefined();
            channel.subscribe(
                (message: Message) => {
                    const responseObject = message.payload as FakeApiCallResponseObject;
                    const response = responseObject.payload as string;
                    expect(response).toBe('Fake Response');
                }
            );

            const requestObject = new FakeApiCallRequestObject(FakeChannel.request, 'GET');
            bus.sendRequestMessageWithId(FakeChannel.request, requestObject, id);
        }
    );

    it('Should exercise postResponse without message args and buildAPIRequest().',
        () => {
            let channel = bus.api.getResponseChannel(FakeChannel.request);
            const id = GeneralUtil.genUUIDShort();

            expect(channel)
                .not
                .toBeUndefined();
            channel.subscribe(
                (message: Message) => {
                    const responseObject = message.payload as FakeApiCallResponseObject;
                    const api = responseObject.payload as APIRequest<string>;
                    expect(api.type).toBe('Fake type');
                    expect(api.payload).toBe('Fake payload');
                    expect(api.version).toBe(123);
                }
            );

            const requestObject = new FakeApiCallRequestObject(FakeChannel.request, 'Fake Request');
            bus.sendRequestMessageWithId(FakeChannel.request, requestObject, id);
        }
    );

    it('Should simulate an autogenerated API call with Rest error (negative test).',
        () => {
            let channel = bus.api.getResponseChannel(FakeChannel.request);
            const id = GeneralUtil.genUUIDShort();
            httpClient.mustFail = true;

            expect(channel)
                .not
                .toBeUndefined();
            channel.subscribe(
                (message: Message) => {
                    expect(message.isError()).toBeTruthy();
                }
            );

            const requestObject = new FakeApiCallRequestObject(FakeChannel.request, 'GET');
            bus.sendRequestMessageWithId(FakeChannel.request, requestObject, id);
        }
    );

    it('Should simulate an autogenerated API call with bad Rest op (negative test).',
        () => {
            let channel = bus.api.getResponseChannel(FakeChannel.request);
            const id = GeneralUtil.genUUIDShort();

            expect(channel)
                .not
                .toBeUndefined();
            channel.subscribe(
                (message: Message) => {
                    expect(message.isError()).toBeTruthy();
                }
            );

            const requestObject = new FakeApiCallRequestObject(FakeChannel.request, 'Bad Op');
            bus.sendRequestMessageWithId(FakeChannel.request, requestObject, id);
        }
    );

    it('Should send GET to the Rest service.',
        () => {
            let channel = bus.api.getResponseChannel(FakeChannel.request);
            const id = GeneralUtil.genUUIDShort();
            const packet: any = {};
            Object.assign(packet, restPayloadGet);

            expect(channel)
                .not
                .toBeUndefined();
            channel.subscribe(
                (message: Message) => {
                    const responseObject = message.payload as FakeRestRelayResponseObject;
                    const payload = responseObject.payload as string;
                    expect(payload).toBe('GET called');
                }
            );

            packet['op'] = 'GET';
            const requestObject = new FakeRestRelayRequestObject(FakeChannel.request, packet);
            bus.sendRequestMessageWithId(FakeChannel.request, requestObject, id);
        }
    );

    it('Should fail on a GET to the Rest service with 401 (negative test).',
        () => {
            let channel = bus.api.getResponseChannel(FakeChannel.request);
            const id = GeneralUtil.genUUIDShort();
            const packet: any = {};
            Object.assign(packet, restPayloadGet);
            httpClient.mustFail = true;
            httpClient.errCode = 401;

            expect(channel)
                .not
                .toBeUndefined();
            channel.subscribe(
                (message: Message) => {
                    expect(message.id).toEqual(id);
                    expect(message.isError()).toBeTruthy();
                }
            );

            packet['op'] = 'GET';
            const requestObject = new FakeRestRelayRequestObject(FakeChannel.request, packet);
            bus.sendRequestMessageWithId(FakeChannel.request, requestObject, id);
        }
    );

    it('Should fail on a GET to the Rest service with something other than 401 (negative branch test).',
        () => {
            let channel = bus.api.getResponseChannel(FakeChannel.request);
            const id = GeneralUtil.genUUIDShort();
            const packet: any = {};
            Object.assign(packet, restPayloadGet);
            httpClient.mustFail = true;
            httpClient.errCode = 500;

            expect(channel)
                .not
                .toBeUndefined();
            channel.subscribe(
                (message: Message) => {
                    expect(message.id).toEqual(id);
                    expect(message.isError()).toBeTruthy();
                }
            );

            packet['op'] = 'GET';
            const requestObject = new FakeRestRelayRequestObject(FakeChannel.request, packet);
            bus.sendRequestMessageWithId(FakeChannel.request, requestObject, id);
        }
    );

    it('Should fail on a bad op to the Rest service (negative test).',
        () => {
            let channel = bus.api.getResponseChannel(FakeChannel.request);
            const id = GeneralUtil.genUUIDShort();
            const packet: any = {};
            Object.assign(packet, restPayloadGet);

            expect(channel)
                .not
                .toBeUndefined();
            channel.subscribe(
                (message: Message) => {
                    expect(message.id).toEqual(id);
                    expect(message.isError()).toBeTruthy();
                }
            );

            packet['op'] = 'Bad Op';
            const requestObject = new FakeRestRelayRequestObject(FakeChannel.request, packet);
            bus.sendRequestMessageWithId(FakeChannel.request, requestObject, id);
        }
    );

    it('Should send POST to the Rest service.',
        () => {
            let channel = bus.api.getResponseChannel(FakeChannel.request);
            const id = GeneralUtil.genUUIDShort();
            const packet: any = {};
            Object.assign(packet, restPayloadPost);

            expect(channel)
                .not
                .toBeUndefined();
            channel.subscribe(
                (message: Message) => {
                    const responseObject = message.payload as FakeRestRelayResponseObject;
                    const payload = responseObject.payload as string;
                    expect(payload).toBe('POST called');
                }
            );

            packet['op'] = 'POST';
            const requestObject = new FakeRestRelayRequestObject(FakeChannel.request, packet);
            bus.sendRequestMessageWithId(FakeChannel.request, requestObject, id);
        }
    );

    it('Should send PUT to the Rest service.',
        () => {
            let channel = bus.api.getResponseChannel(FakeChannel.request);
            const id = GeneralUtil.genUUIDShort();
            const packet: any = {};
            Object.assign(packet, restPayloadPost);

            expect(channel)
                .not
                .toBeUndefined();
            channel.subscribe(
                (message: Message) => {
                    const responseObject = message.payload as FakeRestRelayResponseObject;
                    const payload = responseObject.payload as string;
                    expect(payload).toBe('PUT called');
                }
            );

            packet['op'] = 'PUT';
            const requestObject = new FakeRestRelayRequestObject(FakeChannel.request, packet);
            bus.sendRequestMessageWithId(FakeChannel.request, requestObject, id);
        }
    );

    it('Should send PATCH to the Rest service.',
        () => {
            let channel = bus.api.getResponseChannel(FakeChannel.request);
            const id = GeneralUtil.genUUIDShort();
            const packet: any = {};
            Object.assign(packet, restPayloadPost);

            expect(channel)
                .not
                .toBeUndefined();
            channel.subscribe(
                (message: Message) => {
                    const responseObject = message.payload as FakeRestRelayResponseObject;
                    const payload = responseObject.payload as string;
                    expect(payload).toBe('PATCH called');
                }
            );

            packet['op'] = 'PATCH';
            const requestObject = new FakeRestRelayRequestObject(FakeChannel.request, packet);
            bus.sendRequestMessageWithId(FakeChannel.request, requestObject, id);
        }
    );

    it('Should send DELETE to the Rest service.',
        () => {
            let channel = bus.api.getResponseChannel(FakeChannel.request);
            const id = GeneralUtil.genUUIDShort();
            const packet: any = {};
            Object.assign(packet, restPayloadPost);

            expect(channel)
                .not
                .toBeUndefined();
            channel.subscribe(
                (message: Message) => {
                    const responseObject = message.payload as FakeRestRelayResponseObject;
                    const payload = responseObject.payload as string;
                    expect(payload).toBe('DELETE called');
                }
            );

            packet['op'] = 'DELETE';
            const requestObject = new FakeRestRelayRequestObject(FakeChannel.request, packet);
            bus.sendRequestMessageWithId(FakeChannel.request, requestObject, id);
        }
    );

    it('Should update headers.',
        () => {
            let channel = bus.api.getResponseChannel(FakeChannel.request);
            const id = GeneralUtil.genUUIDShort();
            const packet: any = {};
            Object.assign(packet, restPayloadPost);

            expect(channel)
                .not
                .toBeUndefined();
            channel.subscribe(
                (message: Message) => {
                    expect(message.isError()).toBeFalsy();
                }
            );

            packet['op'] = 'UPDATE_HEADERS';
            const requestObject = new FakeRestRelayRequestObject(FakeChannel.request, packet);
            bus.sendRequestMessageWithId(FakeChannel.request, requestObject, id);
        }
    );
});
