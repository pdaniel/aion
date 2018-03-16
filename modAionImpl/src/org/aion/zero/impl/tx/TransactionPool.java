package org.aion.zero.impl.tx;

import org.aion.base.type.Address;
import org.aion.base.type.ITransaction;
import org.aion.base.util.ByteArrayWrapper;
import org.aion.zero.impl.AionBlockchainImpl;
import org.aion.zero.impl.core.IAionBlockchain;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TransactionPool<T extends ITransaction> {

    /**
     * Represents the list of all transactions currently in the pool
     * (this contains both pending and queued pools)
     */
    private HashMap<ByteArrayWrapper, T> globalPool = new HashMap<>();

    /**
     * Pool for all transactions that are immediately executable,
     * these transactions should not invoke any rejections from
     * the VM
     */
    private Map<Address, AccountTxList<T>> pendingPool = new HashMap<>();

    /**
     * Transactions that are not immediately executable
     */
    private Map<Address, AccountTxList<T>> queuedPool = new HashMap<>();

    /**
     * From here, we should be able to access all the state that we require, this
     * is mainly limited to retrieving the best block and updating the blockchain
     * on any re-organizations
     */
    private final AionBlockchainImpl blockchain;

    /**
     * Pretty much a replacement for nonceManager
     */
    private Map<Address, Long> nonceMap = new HashMap<>();

    /**
     * Basically determines the lifetime of a transaction, given that they were
     */
    private long cullDuration;


    // Utility classes, not directly tied to the state of the pool

    ScheduledExecutorService reaperExecutor = Executors.newSingleThreadScheduledExecutor();

    public TransactionPool(AionBlockchainImpl blockchain, long cullDurationSeconds) {
        this.blockchain = blockchain;
        this.cullDuration = cullDurationSeconds;
    }

    /**
     * Starts the reaper thread to destroy long-lasting transactions
     */
    public synchronized void initialize() {
        reaperExecutor.scheduleAtFixedRate(() -> {
            cullOldQueuedTransactions(System.currentTimeMillis() - cullDuration);
        }, 60L, 60L, TimeUnit.SECONDS);
    }

    public synchronized boolean addLocal(T transaction) {
        TransactionStatus<T> status = new TransactionStatus<>(transaction, false);
        return add(status);
    }

    public synchronized boolean addRemote(T transaction) {
        TransactionStatus<T> status = new TransactionStatus<>(transaction, true);
        return add(status);
    }

    /**
     * Adds a single transaction to the pending pool if possible, if not adds
     * directly to queue
     * @param status
     * @return
     */
    private boolean add(TransactionStatus<T> status) {

    }

    public synchronized long getNonce(Address address) {
        Long nonce;
        // nonceMap is not a cache! its only intended to manage pending nonces
        if ((nonce = this.nonceMap.get(address)) != null)
            return nonce;
        return this.blockchain.getRepository().getNonce(address).longValue();
    }

    /**
     * Called by the reaper thread to destroy transactions that have been in
     * the queue for an exorbitant amount of time
     *
     * @return {@code amount} of transactions culled
     */
    private synchronized boolean cullOldQueuedTransactions(long lowerBound) {
        // only reap queue for now, since pending transactions should be
        // processed shortly (atleast thats the assumption)
        Iterator<AccountTxList<T>> it = this.queuedPool.values().iterator();
        while(it.hasNext()) {
            AccountTxList<T> l = it.next();
            if (l.getLastTouched() < lowerBound) {
                for (TransactionStatus<T> status : l.getAll()) {
                    // TODO: major optimizations can happen here
                    // using byte array wrappers incurs a copy
                    globalPool.remove(new ByteArrayWrapper(status.transaction.getHash()));
                }
            }
        }
        return true;
    }
}
