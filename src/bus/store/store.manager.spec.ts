/*
 * Copyright 2018-2020 VMware, Inc.
 * SPDX-License-Identifier: BSD-2-Clause
 */

import { EventBus } from '../../bus.api';
import { Logger, LogLevel } from '../../log';
import { BusStore } from '../../store.api';
import { BusTestUtil } from '../../util/test.util';
import { StoreImpl } from './store';

describe('Store Manager [store/store.manager]', () => {

    let bus: EventBus;
    let log: Logger;

    beforeEach(
        () => {
            bus = null;
            bus = BusTestUtil.bootBusWithOptions(LogLevel.Off, true);
            bus.api.loggerInstance.setStylingVisble(false);
            bus.api.logger().silent(true);
            log = bus.api.logger();
        }
    );

    it('destroyStore() method calls store`s closeStore() method', () => {
        const testStore = bus.stores.createStore('testStore') as StoreImpl<string>;
        spyOn(testStore, 'closeStore');
        bus.stores.destroyStore('testStore');
        expect(testStore.closeStore).toHaveBeenCalled();
    });

    it('Check readyJoin works', (done) => {

        bus.stores.readyJoin(['ember', 'fox']).whenReady(
            () => {
                done();
            }
        );

        bus.stores.createStore('ember').initialize();
        bus.stores.createStore('fox').initialize();

    });

    it('Check readyJoin works and values come through', (done) => {

        bus.stores.readyJoin(['ember', 'fox']).whenReady(
            (stores: Array<BusStore<any>>) => {
                expect(stores.length).toEqual(3);
                expect(stores[1].get('fox')).toEqual('honk');
                done();
            }
        );

        const store1 = bus.stores.createStore('ember');
        const store2 = bus.stores.createStore('fox');

        store1.put('fox', 'honk', null);
        store1.initialize();
        store2.initialize();

    });

    it('check we can wipe all stores', () => {

        const store1 = bus.stores.createStore('ember');
        const store2 = bus.stores.createStore('fox');
        store1.put('tip', 'top', null);
        store1.put('cap', 'sap', null);
        store2.put('spit', 'spot', null);
        store2.put('pit', 'pot', null);

        let itemCount = store1.allValues().length + store2.allValues().length;

        expect(bus.stores.getAllStores().length).toEqual(3);
        expect(itemCount).toEqual(4);

        bus.stores.wipeAllStores();
        expect(bus.stores.getAllStores().length).toEqual(3);

        itemCount = store1.allValues().length + store2.allValues().length;
        expect(itemCount).toEqual(0);

    });
});
