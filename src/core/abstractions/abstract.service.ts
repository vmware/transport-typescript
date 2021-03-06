/*
 * Copyright 2018-2020 VMware, Inc.
 * SPDX-License-Identifier: BSD-2-Clause
 */

import { ChannelName, MessageArgs, MessageHandler, ORG_ID, ORGS, SentFrom } from '../../bus.api';
import { AbstractBase } from './abstract.base';
import { HttpRequest, RestError, RestObject } from '../services/rest/rest.model';
import { APIRequest } from '../model/request.model';
import { BusStore, UUID } from '../../bus';
import { GeneralUtil } from '../../util/util';
import { GeneralError } from '../model/error.model';
import { ApiObject } from './abstract.apiobject';
import { AbstractMessageObject } from './abstract.messageobject';
import { RestService } from '../services/rest/rest.service';
import { Subscription } from 'rxjs';
import { FabricService } from './fabric.service';

export const SERVICE_ERROR = 505;
export type RequestorArguments = MessageArgs;

const HTTP_REQUEST_MAP: Array<[string, HttpRequest]> = [
    ['GET', HttpRequest.Get],
    ['POST', HttpRequest.Post],
    ['PATCH', HttpRequest.Patch],
    ['PUT', HttpRequest.Put],
    ['DELETE', HttpRequest.Delete],
    ['UPDATE_HEADERS', HttpRequest.UpdateGlobalHeaders],
    ['HOST_OPTIONS', HttpRequest.SetRestServiceHostOptions],
    ['CORS_OPTIONS', HttpRequest.DisableCORSAndCredentials],
    ['CORS_CREDENTIALS_OPTIONS', HttpRequest.ConfigureCORSAndCredentials]

];

/*
 * These three synthetic types are used to define the lambdas that are passed to and from the
 * autogenerated API layer via "apigen". They form the generic handlers used by "apiBridge".
 */
type SuccessHandler = (apiObject: ApiObject<any, any>, response: any, requestArgs?: MessageArgs) => void;
type ErrorHandler = (apiObject: ApiObject<any, any>, err: RestError, requestArgs?: MessageArgs) => void;
type ApiFunction = (apiObject: ApiObject<any, any>,
                    httpOp: string,
                    uri: string,
                    body: any,
                    successHandler: SuccessHandler,
                    failureHandler: ErrorHandler,
                    apiClass: string,
                    apiArgs?: MessageArgs) => void;

/*
 * Analogous to the above three synthetic types to define lambdas that are used for communication between
 * the service layer and the API layer, the following three synthetic types define the lambdas that
 * are used for communication between two services. e.g. a service calling the RestService would end up as
 * a service-to-service call using these functions.
 */
type CallSuccessHandler = (response: AbstractMessageObject<any, any>, args?: MessageArgs) => void;
type CallFailureHandler = (error: RestError, args?: MessageArgs) => void;
type ServiceCallFunction = (requestChannel: string,
                            requestObject: AbstractMessageObject<any, any>,
                            successHandler: CallSuccessHandler,
                            failureHandler: CallFailureHandler,
                            messageArgs: MessageArgs) => void;


/**
 * This class extends MessageArgs in order to be able to pass state to the handler lambda
 */

export class CallerArgs implements MessageArgs {
    constructor(public uuid: UUID, public from: SentFrom = '🔋SYNTHETIC🔋', public version: number = 1) {
    }
}

/**
 * This is an abstract service that encapsulates messagebus handling and implements some
 * of the more commonly used methods by the derived services. The derived classes provide
 * handlers for when a all is received or when there is a response from a ReST all.
 * The error handler can be overridden in the derived class.
 *
 * ReqT is the type of the all payload to the service (e.g. RolesRequestObject)
 * RespT is the type of the response payload from the service (e.g. RolesResponseObject)
 */
export abstract class AbstractService<ReqT, RespT> extends AbstractBase implements FabricService {
    protected serviceError: RestError;

    protected apiBridge: ApiFunction;
    protected apiSuccessHandler: SuccessHandler;
    protected apiFailureHandler: ErrorHandler;
    protected serviceCall: ServiceCallFunction;

    protected requestConverterMap: Map<string, HttpRequest>;
    protected readonly serviceChannel: ChannelName;
    protected readonly broadcastChannel: ChannelName;

    protected $host: string | undefined;    // This allows for dynamically customizing host segment in URIs prior to ReST service calls

    protected requestStream: MessageHandler;
    protected requestStreamSub: Subscription;
    public readonly id: UUID;

    /**
     * super()
     *
     * @param name - name of the derived service (e.g. 'task.service'
     * @param serviceChannel - channel on which to listen for requests and send responses for the derived service
     * @param broadcastChannel - channel on which to broadcast for all listeners.
     */
    protected constructor(name: string, serviceChannel: ChannelName, broadcastChannel?: ChannelName) {

        super(name);

        this.id = GeneralUtil.genUUID();

        // set the service channel.
        this.serviceChannel = serviceChannel;

        // set the broadcast channel.
        this.broadcastChannel = broadcastChannel;

        this.serviceError = new RestError('Invalid Service APIRequest!', SERVICE_ERROR, '');
        this.requestConverterMap = new Map<string, HttpRequest>(HTTP_REQUEST_MAP);

        this.initializeServiceCallHandling();   // create the serviceCall lambda
        this.initializeApiHandling();           // create the apiBridge lambda
        this.listenToRequestStream();
    }

    private listenToRequestStream() {
        this.log.info(`🌎 Service Adaptor: ${this.name} (${this.id}) online and listening on '${this.serviceChannel}'`, this.getName());
        this.requestStream = this.bus.listenRequestStream<ReqT>(this.serviceChannel, this.getName());
        this.requestStreamSub = this.requestStream.handle((requestObject: ReqT, args: RequestorArguments) => {
            this.handleServiceRequest(requestObject, args);
        });
    }

// Required method in the derived service
    protected abstract handleServiceRequest(requestObject: ReqT, requestArgs?: MessageArgs): void;

    /**
     * RestError to use for invalid requests
     *
     * @returns {RestError}
     */
    protected get serviceRequestError(): RestError {
        return this.serviceError;
    }

    /**
     *  Method to send a response object to the client of the service
     * @param {string} channel to respond to
     * @param {any} responseObject response object to send
     * @param {MessageArgs} args optional arguments to pass.
     */
    protected postResponse(channel: string, responseObject: any, args?: MessageArgs): void {
        if (args) {
            this.bus.sendResponseMessageWithIdAndVersion(channel, responseObject, args.uuid, args.version, args.from);
        } else {
            this.bus.sendResponseMessage(channel, responseObject, this.getName());
        }
    }

    /**
     * Method to send a RestError to the client of the service
     * @param {string} channel channel to sent error to.
     * @param {RestError} err returned from rest service.
     * @param {MessageArgs} args optional arguments to pass.
     */
    protected postError(channel: string, err: GeneralError, args?: MessageArgs): void {
        if (args) {
            this.bus.sendErrorMessageWithIdAndVersion(channel, err, args.uuid, args.version, args.from);
        } else {
            this.log.error('📍 postError - 📍NO ARGS!📍' + channel, this.getName());
            this.bus.sendErrorMessage(channel, err, this.getName());
        }
    }

    /**
     * Build a API request command object
     *
     * @param {string} requestCommand
     * @param {T} payload
     * @param {UUID} uuid
     * @param {number} version
     * @returns {APIRequest<T>}
     */
    protected buildAPIRequest<T>(requestCommand: string, payload: T,
                                 uuid: UUID = GeneralUtil.genUUID(),
                                 version: number = 1): APIRequest<T> {

        return new APIRequest(requestCommand, payload, uuid, version);
    }

    /**
     * The "serviceCall" lambda is used to send messages between services, abstracting the message bus.
     */
    private initializeServiceCallHandling() {
        this.serviceCall = (channel: string,
                            requestObject: AbstractMessageObject<ReqT, any>,
                            successHandler: CallSuccessHandler,
                            failureHandler: CallFailureHandler,
                            callerArgs: MessageArgs) => {

            const messageHandler = this.bus
                .requestOnceWithId(
                    GeneralUtil.genUUID(),
                    channel,
                    requestObject,
                    null,
                    this.getName()
                );

            messageHandler
                .handle((callResponseObject: AbstractMessageObject<RespT, any>, args: MessageArgs) => {
                        // We come here on response from the called service
                        // We call the success handler that was provided by the API Handler.
                        successHandler(callResponseObject, args);
                    },
                    (err: RestError) => {
                        failureHandler(err, callerArgs);
                    }
                );
        };
    }

    /**
     * Initialize lambda function context for use by the API layer autogenerated by "apigen"
     *
     * In order to pass functions around in a typesafe fashion while preserving 'this', is to use lambdas, which
     * preserves the context of protected functions, and unlike bind(), does not lose the type information.
     * Basically, the lambda is stored in a class variable that can be passed around at will and be invoked exactly
     * like a function reference. This solves the non-intuitive handling of 'this' by javascript.
     */
    private initializeApiHandling() {

        // Implement function as an arrow function to preserve 'this'
        // Caution: Read this code below carefully - contains complex flows in the abstraction
        // See: https://confluence.eng.vmware.com/display/SKYS/Autogenerated+UI+Applications
        //
        this.apiBridge = (apiObject: ApiObject<ReqT, RespT>,
                          httpOp: string,
                          uri: string,
                          body: any,
                          successHandler: SuccessHandler,
                          failureHandler: ErrorHandler,
                          apiClass: string,
                          apiArgs?: MessageArgs) => {

            if (!this.requestConverterMap.has(httpOp)) {
                this.log.error('FATAL: Invalid RestRequest provided to AbstractService.apiBridge(): ' + httpOp,
                    this.getName());
                return;
            }

            if (!apiArgs) {
                this.log.error('FATAL: ApiBridge called without MessageArgs in ' + this.getName(), this.getName());
            }

            // Services like NSX-T dynamically change the host for their API. By setting $host in the derived service,
            // the URI passed to RestService is modified to prepend an alternate host. This can be done prior to every
            // API call.

            if (this.$host) {
                uri = this.$host + uri;
            }

            // Prepare the payload for RestService
            const restRequestObject = new RestObject(
                this.requestConverterMap.get(httpOp),
                uri,                    // fully formed URI with path params and query params
                body,
                apiObject.getHeaders(), // headers
                {},   // queryStringParams
                {},   // pathParams
                apiClass,
                this.getName()
            );

            this.serviceCall(RestService.channel, restRequestObject,
                (restResponseObject: RestObject) => {
                    successHandler(apiObject, restResponseObject.response, apiArgs);
                },
                (err: RestError) => {
                    failureHandler(apiObject, err, apiArgs);
                }, apiArgs);
        };

        // These are the handlers that should be provided by the service, or called at the bottom of their
        // own handlers if they want to intercept the response to the µApplication layer

        this.apiSuccessHandler = (apiObject: ApiObject<ReqT, RespT>, payload: any, args?: MessageArgs) => {
            apiObject.responseObject.payload = payload;
            this.postResponse(this.serviceChannel, apiObject.responseObject, args);
        };

        this.apiFailureHandler = (apiObject: ApiObject<ReqT, RespT>, err: RestError, args?: MessageArgs) => {
            this.postError(this.serviceChannel, err, args);
        };
    }

    /**
     * Required for any VMware Cloud Services API.
     */
    protected get callerOrgId() {
        const store: BusStore<string> = this.storeManager.getStore<string>(ORGS);
        let orgId: string = 'orgId not set!';
        if (store) {
            orgId = store.get(ORG_ID);
        }
        return orgId;
    }

    /**
     * Helper function to generate an API object
     *
     * @param requestObject
     * @param responseObject
     */
    protected genApiObject(requestObject: AbstractMessageObject<any, any>,
                           responseObject: AbstractMessageObject<any, any>) {
        return new ApiObject<any, any>(
            requestObject,
            responseObject
        );
    }

    /**
     * Broadcast message to all subscribers on channel.
     * @param channel
     * @param payload
     */
    protected broadcastResponse(channel: string, payload: any) {
        this.log.debug('Broadcasting response/notification to ' + channel, this.getName());
        this.bus.sendResponseMessage(channel, payload, this.getName());
    }

    /**
     * Alias for broadcastResponse()
     *
     * @param channel
     * @param notification
     */
    protected broadcastNotification<N>(channel: string, notification: N) {
       this.broadcastResponse(channel, notification);
    }

    /**
     * Knock the service offline.
     */
    public offline(): void {
        this.requestStreamSub.unsubscribe();
        this.requestStream.close();
        this.log.info(`Service ${this.getName()} has been knocked offline.`, this.getName());
    }

    /**
     * Bring service online.
     */
    public online(): void {
        this.listenToRequestStream();
        this.log.info(`Service ${this.getName()} is now online and listening for requests.`, this.getName());
    }
}
