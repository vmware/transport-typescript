/**
 * Copyright(c) VMware Inc. 2017
 */

import { UUID } from './store.model';
import { BusStore, StoreStream, MutateStream } from '../../store.api';
import { MessageFunction } from '../../bus.api';
import { EventBus } from '../../bus.api';
import { Logger } from '../../log';
import { BusTestUtil } from '../../util/test.util';

enum State {
    Created = 'Created',
    Updated = 'Updated',
    Deleted = 'Deleted'
}

enum Mutate {
    Update = 'Update',
    AddStuff = 'AddStuff',
    RemoveStuff = 'RemoveStuff'
}

describe('BusStore [store/store.model]', () => {
    let bus: EventBus;
    let log: Logger;

    beforeEach(() => {
        bus = BusTestUtil.bootBus();
        bus.api.silenceLog(true);
        bus.api.suppressLog(true);
        bus.stores.createStore('string');
        log = bus.api.logger();
    });

    afterEach(() => {
        bus.stores.getStore('string').reset();
        bus.api.destroyAllChannels();
    });

    it('Check cache has been set up correctly', () => {
        expect(bus.stores.getStore('string')).not.toBeNull();
        expect(bus.stores.getStore('string').populate(new Map<UUID, string>())).toBeTruthy();
    });

    it('check put() and retrive() work correctly', () => {
        bus.stores.getStore('string').put('123', 'chickie & fox', State.Created);
        expect(bus.stores.getStore('string').get('123')).toEqual('chickie & fox');
        expect(bus.stores.getStore('string').get('456')).toBeUndefined();
    });

    it('check remove() works correctly', (done) => {
        const cache: BusStore<string> = bus.stores.getStore('string');

        cache.onChange<State.Deleted>('123', State.Deleted)
            .subscribe(
            (s: string) => {
                expect(s).toEqual('chickie & fox');
                expect(cache.get('123')).toBeUndefined();
                done();
            }
            );

        cache.put('123', 'chickie & fox', State.Created);
        expect(cache.get('123')).toEqual('chickie & fox');
        expect(cache.remove('789', State.Deleted)).toBeFalsy();
        expect(cache.remove('123', State.Deleted)).toBeTruthy();
    });

    it('check populate() works correctly', () => {
        const cache: BusStore<string> = bus.stores.getStore('string');
        const data: Map<UUID, string> = new Map<UUID, string>();
        data.set('123', 'miss you so much maggie pop');
        data.set('456', 'you were the best boy');

        expect(cache.populate(data)).toBeTruthy();
        expect(cache.get('123')).toEqual('miss you so much maggie pop');
        expect(cache.get('456')).toEqual('you were the best boy');
        expect(cache.populate(data)).toBeFalsy();

    });

    it('check reset() works correctly', () => {
        const cache: BusStore<string> = bus.stores.getStore('string');

        cache.put('123', 'chickie & fox', State.Created);
        expect(cache.get('123')).toEqual('chickie & fox');
        cache.put('456', 'maggie pop', State.Created);
        expect(cache.get('456')).toEqual('maggie pop');
        cache.reset();
        expect(cache.get('123')).toBeUndefined();
        expect(cache.get('456')).toBeUndefined();
    });

    it('check allValues() works correctly', () => {
        const cache: BusStore<string> = bus.stores.getStore('string');

        cache.put('123', 'pup pup pup', State.Created);
        cache.put('456', 'pop pop pop', State.Created);
        cache.put('789', 'woof woof bark', State.Created);

        expect(cache.allValues().length).toEqual(3);

        cache.reset();

        expect(cache.allValues().length).toEqual(0);
        expect(cache.get('123')).toBeUndefined();
        expect(cache.get('456')).toBeUndefined();
    });

    it('check allValuesAsMap() works correctly', () => {
        const cache: BusStore<string> = bus.stores.getStore('string');

        cache.put('123', 'tip top', State.Created);
        cache.put('456', 'clip clop', State.Created);

        const map = cache.allValuesAsMap();

        expect(map.size).toEqual(2);
        expect(map.get('123')).toEqual('tip top');
        expect(map.get('456')).toEqual('clip clop');

        /* check notification does not alter cache */
        map.set('123', 'bacon!');

        expect(cache.allValuesAsMap().get('123')).not.toEqual('bacon!');
        expect(cache.allValuesAsMap().get('123')).toEqual('tip top');

    });

    it('check onChange() works correctly', (done) => {

        const cache: BusStore<Dog> = bus.stores.createStore('Dog');

        let counter: number = 0;

        cache.onChange<State.Created>('magnum', State.Created)
            .subscribe(
            (d: Dog) => {
                expect(d.dogName).toEqual('maggie');
                expect(d.dogAge).toEqual(12);
                expect(d.dogPhrase).toEqual('get the kitty');
                counter++;
            }
            );

        cache.onChange<State.Updated>('fox', State.Updated)
            .subscribe(
            (d: Dog) => {
                expect(d.dogName).toEqual('foxy pop');
                expect(d.dogAge).toEqual(11);
                expect(d.dogPhrase).toEqual('get out of the pantry');
                counter++;
            }
            );

        cache.onChange<State.Deleted>('cotton', State.Deleted)
            .subscribe(
            (d: Dog) => {
                expect(d.dogName).toEqual('chickie');
                expect(d.dogAge).toEqual(6);
                expect(d.dogPhrase).toEqual('where is the kitty');
                counter++;
                if (counter === 3) {
                    done();
                }
            }
            );

        cache.put(
            'magnum',
            new Dog('maggie', 12, 'get the kitty'),
            State.Created
        );
        cache.put(
            'fox',
            new Dog('foxy pop', 11, 'get out of the pantry'),
            State.Updated
        );
        cache.put(
            'cotton',
            new Dog('chickie', 6, 'where is the kitty'),
            State.Deleted
        );

    });

    it('onChange() works with no success handler supplied', (done) => {
        spyOn(log, 'error').and.callThrough();

        const store: BusStore<string> = bus.stores.createStore('dog');
        store.onChange<State.Created>('magnum', State.Created)
            .subscribe(null);

        store.put('magnum', 'maggie', State.Created);

        bus.api.tickEventLoop(
            () => {
                expect(log.error)
                    .toHaveBeenCalledWith('unable to handle cache stream event, ' +
                        'no handler provided.', 'StoreStream');
                done();
            },
            20
        );
    });

    it('onChange() works with all types', (done) => {
        
        let count = 0;
        const store: BusStore<string> = bus.stores.createStore('dog');
        store.onChange<State.Created>('magnum')
            .subscribe(
                () => {
                  count++;
                  if (count === 3) {
                      done();
                  }  
                }
            );

        store.put('magnum', 'maggie', State.Created);
        store.put('magnum', 'maggie', State.Updated);
        store.put('magnum', 'maggie', 'fart');

    });

    it('onChange() works with unsubscriptions', (done) => {

        let counter = 0;
        const store: BusStore<string> = bus.stores.createStore('dog');
        const sub = store.onChange<State.Updated>('magnum', State.Updated);
        sub.unsubscribe(); // nothing should happen because we're not subscribed yet.
        
        sub.subscribe(
            () => {
                counter++;
            }
        );

        store.put('magnum', 'maggie', State.Created);
        store.put('magnum', 'maggie', State.Updated);
        store.put('magnum', 'maggie', State.Updated);
        store.put('magnum', 'maggie', State.Updated);
        
        bus.api.tickEventLoop(
            () => {
                expect(counter).toEqual(3);
                sub.unsubscribe();
                store.put('magnum', 'maggie', State.Updated);
                store.put('magnum', 'maggie', State.Updated);
                store.put('magnum', 'maggie', State.Updated);
                store.put('magnum', 'maggie', State.Updated);
            },
            10
        );

        bus.api.tickEventLoop(
            () => {
                expect(counter).toEqual(3);
                done();
            },
            20
        );

    });

    it('check onAllChanges() works correctly', (done) => {

        const cache = bus.stores.createStore('Dog');

        let counter: number = 0;


        cache.onAllChanges<State.Created>(new Dog(), State.Created)
            .subscribe(
            () => {
                counter++;
            }
            );

        cache.onAllChanges<State.Updated>(new Dog(), State.Updated)
            .subscribe(
            () => {
                counter++;
                if (counter === 7) {
                    done();
                }
            }
            );

        cache.put(
            'something-else',
            'not a dog!',
            State.Created
        );
        cache.put(
            'magnum',
            new Dog('maggie', 12, 'get the kitty'),
            State.Created
        );
        cache.put(
            'fox',
            new Dog('foxy pop', 11, 'get out of the pantry'),
            State.Created
        );
        cache.put(
            'cotton',
            new Dog('chickie', 6, 'where is the kitty'),
            State.Created
        );
        cache.put(
            'something-else-again',
            'not a dog either!',
            State.Created
        );
        cache.put(
            'fox',
            new Dog('foxy pop', 11, 'get off the couch!'),
            State.Updated
        );
        cache.put(
            'cotton',
            new Dog('chickie', 6, 'want to go for a walk?'),
            State.Updated
        );

    });

    it('check onAllChanges() works correctly with multiple states', (done) => {

        const cache: BusStore<Dog> = bus.stores.createStore('Dog');

        let counter: number = 0;

        listen<State, Dog>(cache, State.Created, State.Updated)
            .subscribe(
            () => {
                counter++;
                if (counter === 2) {
                    done();
                }
            }
            );

        cache.put(
            'fox',
            new Dog('foxy pop', 11, 'get out of the pantry'),
            State.Created
        );
        cache.put(
            'fox',
            new Dog('foxy pop', 11, 'get off the couch!'),
            State.Updated
        );

    });

    it('check onAllChanges() works correctly with all states', (done) => {

        const cache: BusStore<Dog> = bus.stores.createStore('Dog');

        let counter: number = 0;

        // handle edge case of wrapper functions passing down muli-args.
        listen<State, Dog>(cache)
            .subscribe(
            () => {
                counter++;
                if (counter === 3) {
                    done();
                }
            }
            );
        cache.put(
            'fox',
            new Dog('foxy pop', 11, 'get out of the pantry'),
            State.Created
        );
        cache.put(
            'fox',
            new Dog('foxy pop', 11, 'get off the couch!'),
            State.Updated
        );

        cache.put(
            'fox',
            new Dog('foxy pop', 11, 'get off the couch!'),
            State.Deleted
        );
    });

    it('check mutate() works with correct success handling', (done) => {

        const cache: BusStore<Dog> = bus.stores.createStore('Dog');

        let d: Dog = new Dog('foxy', 11, 'eat it, not bury it');
        cache.put('fox', d, State.Created);

        const mutateStream: MutateStream<Dog, string> = cache.onMutationRequest(new Dog(), Mutate.Update);
        mutateStream.subscribe(
            (dog: Dog) => {
                expect(dog.dogName).toEqual('foxy');
                expect(dog.dogPhrase).toEqual('eat it, not bury it');

                // some task was done and the mutation was a success. let the caller know.
                dog.dogPhrase = 'ok, now you can eat it!';
                mutateStream.success(dog);
            }
        );

        cache.mutate(d, Mutate.Update,
            () => {
                expect(d.dogPhrase).toEqual('ok, now you can eat it!');
                done();
            }
        );
    });

    it('check mutate() works with all mutation types', (done) => {

        const cache: BusStore<Dog> = bus.stores.createStore('Dog');

        let d: Dog = new Dog('foxy', 11, 'eat it, not bury it');
        cache.put('fox', d, State.Created);
        let counter = 0;
        const mutateStream: MutateStream<Dog, string> = cache.onMutationRequest(new Dog());
        mutateStream.subscribe(
            () => {
                counter++;
            }
        );

        cache.mutate(d, Mutate.Update, null);
        cache.mutate(d, Mutate.RemoveStuff, null);
        cache.mutate(d, Mutate.AddStuff, null);

        bus.api.tickEventLoop(
            () => {
                expect(counter).toEqual(3);
                done();
            }
            , 20
        );        
    });

    it('check mutate() works with correct logging, without success handler.', (done) => {

        spyOn(log, 'error').and.callThrough();
        const cache: BusStore<Dog> = bus.stores.createStore('Dog');

        let d: Dog = new Dog('foxy', 11, 'eat it, not bury it');
        cache.put('fox', d, State.Created);

        const mutateStream: MutateStream<Dog, string> = cache.onMutationRequest(new Dog(), Mutate.Update);
        mutateStream.subscribe(
            (dog: Dog) => {
                expect(dog.dogName).toEqual('foxy');
                expect(dog.dogPhrase).toEqual('eat it, not bury it');

                // some task was done and the mutation was a success. let the caller know.
                dog.dogPhrase = 'ok, now you can eat it!';
                mutateStream.success(dog);
            }
        );
       
        cache.mutate(d, Mutate.Update, null);

        bus.api.tickEventLoop(
            () => {
                expect(log.error)
                    .toHaveBeenCalledWith('unable to send success event back to mutator, ' +
                        'no success handler provided.', 'MutateStream');
                done();
            }
            , 20
        );
    });


    it('check mutate() works without correct success handling', (done) => {
        spyOn(log, 'error').and.callThrough();
        const cache: BusStore<Dog> = bus.stores.createStore('Dog');

        let d: Dog = new Dog('foxy', 11, 'eat it, not bury it');
        cache.put('fox', d, State.Created);

        const mutateStream: MutateStream<Dog, string> = cache.onMutationRequest(new Dog(), Mutate.Update);
        mutateStream.subscribe(null);

        cache.mutate(d, Mutate.Update, null);

        bus.api.tickEventLoop(
            () => {
                expect(log.error).toHaveBeenCalledWith('unable to handle ' +
                    'cache stream event, no handler provided.', 'MutateStream');
                done();
            }
            , 50
        );
    });

    it('check mutate() works with correct error handling', (done) => {

        const cache: BusStore<Dog> = bus.stores.createStore('Dog');

        let d: Dog = new Dog('foxy', 11, 'eat it, not bury it');
        cache.put('fox', d, State.Created);

        const mutateStream: MutateStream<Dog, string> = cache.onMutationRequest(new Dog(), Mutate.Update);
        mutateStream.subscribe(
            (dog: Dog) => {
                expect(dog.dogName).toEqual('foxy');
                expect(dog.dogPhrase).toEqual('eat it, not bury it');

                // something failed with the mutate, throw an error to the caller.
                mutateStream.error('something went wrong');
            }
        );

        cache.mutate(d, Mutate.Update, null,
            (e: string) => {
                expect(e).toEqual('something went wrong');
                done();
            }
        );
    });

    it('check mutate() works with correct error logging when no error handler provided', (done) => {

        spyOn(log, 'error').and.callThrough();
        const cache: BusStore<Dog> = bus.stores.createStore('Dog');

        let d: Dog = new Dog('foxy', 11, 'eat it, not bury it');
        cache.put('fox', d, State.Created);

        const mutateStream: MutateStream<Dog, string> = cache.onMutationRequest(new Dog(), Mutate.Update);
        mutateStream.subscribe(
            (dog: Dog) => {
                expect(dog.dogName).toEqual('foxy');
                expect(dog.dogPhrase).toEqual('eat it, not bury it');

                // something failed with the mutate, throw an error to the caller.
                mutateStream.error('something went wrong');
            }
        );

        cache.mutate(d, Mutate.Update, null);
        bus.api.tickEventLoop(
            () => {
                expect(log.error)
                    .toHaveBeenCalledWith('unable to send error event back to' +
                        ' mutator, no error handler provided.', 'MutateStream');
                done();
            }
            , 50
        );
    });


    it('check mutate() and notifyOnMutation() works correctly', (done) => {

        const cache: BusStore<Dog> = bus.stores.createStore('Dog');

        let d: Dog = new Dog('maggie', 12, 'get the kitty');
        cache.put('magnum', d, State.Created);

        cache.onMutationRequest<Mutate.Update>(new Dog(), Mutate.Update)
            .subscribe(
            (dog: Dog) => {
                expect(dog.dogName).toEqual('maggie');
                expect(dog.dogPhrase).toEqual('get the kitty');

                // mutate!
                dog.dogName = 'maggles';
                dog.dogPhrase = 'where is your ball?';

                cache.put('magnum', d, State.Updated);
            }
            );

        cache.onChange<State.Updated>('magnum', State.Updated)
            .subscribe(
            (dog: Dog) => {
                expect(dog.dogName).toEqual('maggles');
                expect(dog.dogPhrase).toEqual('where is your ball?');
                done();
            }
            );

        cache.mutate(d, Mutate.Update, null);

    });

    it('check mutatorErrorHandler works so mutator can send errors back to subscribers.', (done) => {

        const cache: BusStore<Dog> = bus.stores.createStore('Dog');

        let d: Dog = new Dog('chicken', 6, 'go find the kitty');
        cache.put('cotton', d, State.Created);

        const stream = cache.onMutationRequest<Mutate.Update>(new Dog(), Mutate.Update);

        stream.subscribe(
            (dog: Dog) => {
                expect(dog.dogName).toEqual('chicken');
                expect(dog.dogPhrase).toEqual('go find the kitty');

                // we have an issue, time to an error;
                stream.error('unable to mutate! something went wrong!');
            }
        );
        const errorHandler: MessageFunction<string> = (v: string) => {
            expect(v).toEqual('unable to mutate! something went wrong!');
            done();
        };

        cache.mutate(d, Mutate.Update, null, errorHandler);

    });


    it('check whenReady() and initialize() work.', (done) => {

        const cache: BusStore<Dog> = bus.stores.createStore('Dog');

        cache.whenReady(
            () => {
                expect(cache.allValues().length).toEqual(1);
                done();
            }
        );

        let d: Dog = new Dog('stinky', 12, 'where is your ball');
        cache.put('magnum', d, State.Created);
        cache.initialize();

    });

    it('check whenReady() and populate() work.', (done) => {

        const cache: BusStore<Dog> = bus.stores.createStore('Dog');

        cache.whenReady(
            () => {
                expect(cache.allValues().length).toEqual(3);
                expect(cache.get('cotton').dogPhrase).toEqual('go find the kitty');
                done();
            }
        );

        const vals: Map<UUID, Dog> = new Map();
        vals.set('magnum', new Dog('stinky', 12, 'where is your ball'));
        vals.set('cotton', new Dog('chickie', 6, 'go find the kitty'));
        vals.set('fox', new Dog('foxy', 11, 'stop that barking'));
        cache.populate(vals);

    });
});

class Dog {
    constructor(
        private name?: string,
        private age?: number,
        private commonPhrase?: string) {

    }

    get dogName() {
        return this.name;
    }

    get dogAge() {
        return this.age;
    }

    get dogPhrase() {
        return this.commonPhrase;
    }

    set dogName(name) {
        this.name = name;
    }

    set dogPhrase(phrase) {
        this.commonPhrase = phrase;
    }

}

function listen<T, B>(cache: BusStore<B>, ...things: T[]): StoreStream<B> {
    let arr = new Array<any>();
    arr.push(new Dog());
    arr = arr.concat(things);
    return cache.onAllChanges.apply(cache, arr);
}