import { ChannelName, MessageArgs, MessageFunction } from '../../bus.api';
import { AbstractBase } from './abstract.base';
import { HttpRequest, RestError, RestObject } from '../services/rest/rest.model';
import { APIRequest } from '../model/request.model';
import { APIResponse } from '../model/response.model';
import { UUID } from '../../bus';
import { GeneralUtil } from '../../util/util';
import { GeneralError } from '../model/error.model';
import { ApiObject } from './abstract.apiobject';
import { AbstractMessageObject } from './abstract.messageobject';
import { RestService } from '../services/rest/rest.service';

export const SERVICE_ERROR = 505;
export type RequestorArguments = MessageArgs;

const HTTP_REQUEST_MAP: Array<[string, HttpRequest]> = [
    ['GET', HttpRequest.Get],
    ['POST', HttpRequest.Post],
    ['PATCH', HttpRequest.Patch],
    ['PUT', HttpRequest.Put],
    ['DELETE', HttpRequest.Delete],
    ['UPDATE_HEADERS', HttpRequest.UpdateGlobalHeaders]
];

/*
 * These three synthetic types are used to define the lambdas that are passed to and from the
 * autogenerated API layer via "apigen". They form the generic handlers used by "apiBridge".
 */
type SuccessHandler = (apiObject: ApiObject<any, any>, response: any, requestArgs?: MessageArgs) => void;
type ErrorHandler = (apiObject: ApiObject<any, any>, err: RestError) => void;
type ApiFunction = (apiObject: ApiObject<any, any>,
                    httpOp: string,
                    uri: string,
                    body: any,
                    successHandler: SuccessHandler,
                    failureHandler: ErrorHandler,
                    apiClass: string) => void;

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
                            failureHandler: CallFailureHandler) => void;

/**
 * This is an abstract service that encapsulates messagebus handling and implements some
 * of the more commonly used methods by the derived services. The derived classes provide
 * handlers for when a all is received or when there is a response from a ReST all.
 * The error handler can be overridden in the derived class.
 *
 * ReqT is the type of the all payload to the service (e.g. RolesRequestObject)
 * RespT is the type of the response payload from the service (e.g. RolesResponseObject)
 */
export abstract class AbstractService<ReqT, RespT> extends AbstractBase {
    protected serviceError: RestError;

    protected apiBridge: ApiFunction;
    protected apiSuccessHandler: SuccessHandler;
    protected apiFailureHandler: ErrorHandler;
    protected serviceCall: ServiceCallFunction;

    protected requestConverterMap: Map<string, HttpRequest>;
    private readonly serviceChannel: ChannelName;

    /**
     * super()
     *
     * @param name - name of the derived service (e.g. 'task.service'
     * @param serviceChannel - channel on which to listen for requests and send responses for the derived service
     */
    protected constructor(name: string, serviceChannel: string) {

        super(name);

        // set the service channel.
        this.serviceChannel = serviceChannel;

        this.serviceError = new RestError('Invalid Service APIRequest!', SERVICE_ERROR, '');
        this.requestConverterMap = new Map<string, HttpRequest>(HTTP_REQUEST_MAP);

        this.initializeServiceCallHandling();   // create the serviceCall lambda
        this.initializeApiHandling();           // create the apiBridge lambda

        this.bus.listenRequestStream(this.serviceChannel, this.getName())
            .handle((requestObject: ReqT, args: RequestorArguments) => {
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
            this.bus.sendErrorMessage(channel, err, this.getName());
        }
    }

    /**
     * Make a galactic command to any services operating on the extended bus
     *
     * @param {APIRequest<GReqT>} request
     * @param {string} channel
     * @param {MessageFunction<GRespT>} handler
     */
    protected makeGalacticRequest<GReqT, GRespT>(request: APIRequest<GReqT>,
                                                 channel: string,
                                                 handler: MessageFunction<GRespT>) {

        this.bus.requestGalactic(channel, request,
            (response: APIResponse<GRespT>, args: MessageArgs) => {
                if (handler) {
                    handler(response.payload, args);
                }
            }
        );
    }

    /**
     * Build a API request command object
     *
     * @param {string} requestType
     * @param {T} payload
     * @param {UUID} uuid
     * @param {number} version
     * @returns {APIRequest<T>}
     */
    protected buildAPIRequest<T>(requestType: string, payload: T,
                                 uuid: UUID = GeneralUtil.genUUID(),
                                 version: number = 1): APIRequest<T> {

        return new APIRequest(requestType, payload, uuid, version);
    }

    /**
     * The "serviceCall" lambda is used to send messages between services, abstracting the message bus.
     */
    private initializeServiceCallHandling() {
        this.serviceCall = (channel: string,
                            requestObject: AbstractMessageObject<ReqT, any>,
                            successHandler: CallSuccessHandler,
                            failureHandler: CallFailureHandler) => {

            const messageHandler = this.bus
                .requestOnceWithId(
                    GeneralUtil.genUUIDShort(),
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
                        failureHandler(err);
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
                          apiClass: string) => {

            if (!this.requestConverterMap.has(httpOp)) {
                this.log.error('FATAL: Invalid RestRequest provided to AbstractService.apiBridge(): ' + httpOp,
                    this.getName());
                return;
            }

            // Prepare the payload for RestService
            const restRequestObject = new RestObject(
                this.requestConverterMap.get(httpOp),
                uri,
                '',
                body,
                apiClass,
                this.getName()
            );

            this.serviceCall(RestService.channel, restRequestObject,
                (restResponseObject: RestObject, args: MessageArgs) => {
                    successHandler(apiObject, restResponseObject.response, args);
                },
                (err: RestError) => {
                    failureHandler(apiObject, err);
                });
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
}
