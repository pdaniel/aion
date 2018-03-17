package org.aion.zero.impl.tx;

import org.aion.base.type.Address;
import org.aion.base.type.ITransaction;
import org.aion.base.util.ByteArrayWrapper;
import org.aion.zero.impl.AionBlockchainImpl;
import org.aion.zero.impl.db.AionRepositoryImpl;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import static org.aion.zero.impl.tx.AccountTxList.ERROR_STATUS;

public class TransactionPool<T extends ITransaction> {

    private enum AddStatus {
        REPLACED,
        ADDED,
        REJECTED
    }

    /**
     * Some VM constants we need to define to correctly pre-calculate some gas
     * costs, see <a href="https://github.com/aionnetwork/aion_fastvm/wiki/Specifications"></a>
     * for reference
     */
    private static class VMConstant {
        public final static long txdatazero = 4;
        public final static long txdatanonzero = 64;
        public final static long ambientTransaction = 21000;
    }

    /**
     * Represents the list of all transactions currently in the pool
     * (this contains both pending and queued pools)
     */
    private final HashMap<ByteArrayWrapper, TransactionStatus<T>> globalPool = new HashMap<>();

    /**
     * Pool for all transactions that are immediately executable,
     * these transactions should not invoke any rejections from
     * the VM
     */
    private final Map<Address, AccountTxList<T>> pendingPool = new HashMap<>();

    /**
     * Transactions that are not immediately executable
     */
    private final Map<Address, AccountTxList<T>> queuedPool = new HashMap<>();

    /**
     * From here, we should be able to access all the state that we require, this
     * is mainly limited to retrieving the best block and updating the blockchain
     * on any re-organizations
     */
    private final AionBlockchainImpl blockchain;

    /**
     * For convenience (and possibly to avoid blockchain swapping repositories on us)
     * Save a pointer to the root repository
     */
    private final AionRepositoryImpl repo;

    /**
     * Pretty much a replacement for nonceManager
     */
    private final Map<Address, Long> nonceMap = new HashMap<>();

    /**
     * A map holding elements of the local pool, these are "special" in that
     * they should get priority, and should be stored and possibly rebroadcasted
     */
    private final Map<ByteArrayWrapper, TransactionStatus<T>> localPool = new HashMap<>();

    /**
     * Basically determines the lifetime of a transaction, given that they were
     */
    private final long cullDuration;

    /**
     * Block energy limit, should be updated at each new block
     */
    private volatile long blockEnergyLimit;

    // Utility classes, not directly tied to the state of the pool

    ScheduledExecutorService reaperExecutor = Executors.newSingleThreadScheduledExecutor();

    public TransactionPool(final AionBlockchainImpl blockchain,
                           final long cullDurationSeconds) {
        this.blockchain = blockchain;
        this.repo = (AionRepositoryImpl) this.blockchain.getRepository();
        this.cullDuration = cullDurationSeconds;
    }

    /**
     * Starts the reaper thread to destroy long-lasting transactions
     */

    public synchronized boolean addLocal(final T transaction) {
        TransactionStatus<T> status = new TransactionStatus<>(transaction, false);
        return addTransaction(status);
    }

    public synchronized boolean addRemote(final T transaction) {
        TransactionStatus<T> status = new TransactionStatus<>(transaction, true);
        return addTransaction(status);
    }

    private boolean addTransaction(final TransactionStatus<T> status) {
        AddStatus retStatus = add(status);

        if (retStatus == AddStatus.REJECTED)
            return false;

        // added new status, changed nonce state need to update
        if (retStatus == AddStatus.ADDED) {
            this.moveToPending(false, status);
        }
        return true;
    }

    /**
     * Adds a single transaction to the pending pool if possible, if not adds
     * directly to queue
     * @param status
     * @return
     */
    private AddStatus add(final TransactionStatus<T> status) {

        // check if it hits our cache
        if (this.globalPool.containsKey(status.hash))
            return AddStatus.REJECTED;

        if (!verifyTransaction(status, this.blockEnergyLimit))
            return AddStatus.REJECTED;

        AccountTxList<T> acc;
        // if account is in pending and replaces a transaction, check if its possible to add directly
        if ((acc = this.pendingPool.get(status.transaction.getFrom())) != null
                && acc.containsNonce(status)) {
            // if we tried to insert but failed, then that means the incoming transaction
            // is underpriced, just abort
            TransactionStatus<T> old;
            if ((old = acc.add(status)) == ERROR_STATUS) {
                return AddStatus.REJECTED;
            }

            // if local add to local pool
            this.localPool.put(status.hash, status);
            // otherwise it was completed successfully, delete the old transaction
            this.globalPool.remove(old.hash);
            this.localPool.remove(old.hash);
            return AddStatus.REPLACED;
        }

        // otherwise, we'll be inserting the transaction into the queue
        AddStatus success = addToQueue(status);
        if (success == AddStatus.ADDED || success == AddStatus.REPLACED) {
            if (!status.isRemote)
                this.localPool.put(status.hash, status);
        }
        return success;
    }

    private AddStatus addToQueue(final TransactionStatus<T> status) {
        if (!this.queuedPool.containsKey(status.transaction.getFrom())) {
            this.queuedPool.put(status.transaction.getFrom(), new AccountTxList<>(false));
        }
        TransactionStatus<T> out = this.queuedPool.get(status.transaction.getFrom()).add(status);

        // was not inserted, this means that an older transaction had a higher
        // price than this transaction, which means we should stop trying to insert
        if (out == ERROR_STATUS) {
            return AddStatus.REJECTED;
        }

        if (out != null) {
            this.globalPool.remove(out.hash);
            this.localPool.remove(out.hash);
        }

        // add new entry
        this.globalPool.put(status.hash, status);
        return out == null ? AddStatus.ADDED : AddStatus.REPLACED;
    }

    /**
     * Validates transactions into the pending pool if possible, at this same time
     * clear the queue pool of any invalid transactions
     *
     * @param filter {@code true} if we want to filter based on current state
     *                           {@code false} otherwise
     */
    private void moveToPending(boolean filter, TransactionStatus<T> ...accs) {

        if (accs == null || accs.length == 0) {
            return;
        }

        for (int i = 0; i < accs.length; i++) {
            Address accAddress = accs[i].transaction.getFrom();
            AccountTxList<T> acc = this.queuedPool.get(accs[i].transaction.getFrom());

            if (filter) {
                long currentNonce = this.repo.getNonce(accAddress).longValueExact();

                // filter nonces in each account, remove transactions that are now
                // below the current nonce (note that this is the current nonce of the actual state)
                List<TransactionStatus<T>> removed = acc.removeBelowNonce(currentNonce);
                for (TransactionStatus<T> r : removed) {
                    this.globalPool.remove(r.hash);
                    this.localPool.remove(r.hash);
                }

                // filter un-executable transactions due to balance
                this.repo.getBalance(accAddress);

                BigInteger currentBalance = this.repo.getBalance(accAddress);
                Map.Entry<List<TransactionStatus<T>>, List<TransactionStatus<T>>> removedInvalidPair =
                        acc.removeUpdateState(currentBalance, this.blockEnergyLimit);

                for (TransactionStatus<T> r: removedInvalidPair.getKey()) {
                    this.globalPool.remove(r.hash);
                    this.localPool.remove(r.hash);
                }
            }

            // move transactions to pending
            List<TransactionStatus<T>> toPending = acc.takeSequential(getNonce(accAddress));
            for (TransactionStatus<T> r : toPending) {
                moveTxToPending(accAddress, r);
            }

            if (acc.isEmpty()) {
                // remove key and value from hashmap
                this.queuedPool.remove(accAddress);
            }
        }
        // TODO: missing logic related to corner cases related to size
        // for now we dont have size!
    }

    // called internally by moveToPending to move a single transaction to pending
    // the state changes here assume only that scenario
    private void moveTxToPending(Address addr, TransactionStatus<T> status) {

        // make sure the list exists!
        AccountTxList<T> acc;
        if ((acc = this.pendingPool.get(addr)) == null) {
            acc = new AccountTxList<>(true);
            this.pendingPool.put(addr, acc);
        }

        TransactionStatus<T> old = acc.add(status);

        // we failed to insert into the list, so drop this transaction
        if (old == ERROR_STATUS) {
            this.globalPool.remove(status.hash);
            this.localPool.remove(status.hash);
        }

        // otherwise remove the old transaction that this replaced
        if (old != null) {
            this.globalPool.remove(old.hash);
            this.localPool.remove(old.hash);
        }

        // TODO: should send an event indicating we added new pending
    }

    // same thing as {@code moveToPending} but applies for pending transactions
    // instead
    private void moveToQueue() {
        // TODO:
    }

    public synchronized long getNonce(final Address address) {
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
    private synchronized boolean cullOldQueuedTransactions(final long lowerBound) {
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

    private void setBlockEnergyLimit(final long energyLimit) {
        this.blockEnergyLimit = energyLimit;
    }

    public static <T extends ITransaction> boolean verifyTransaction(
            final TransactionStatus<T> txStatus,
            final long blockEnergyLimit) {
        final T transaction = txStatus.transaction;

        // structural checks
        if (    transaction.getHash() == null ||
                transaction.getNonce() == null ||
                transaction.getNrgPrice() < 0 ||
                transaction.getNrg() < 0 ||
                transaction.getNrgConsume() < 0 ||
                transaction.getTo() == null ||
                transaction.getFrom() == null ||
                transaction.getValue() == null ||
                transaction.getData() == null)
            return false;

        // more structural checks
        if (transaction.getHash().length != 32 ||
                transaction.getValue().length < 32)
            return false;

        // simple verification checks
        // TODO: we dont do size checks yet
        if (transaction.getNrg() > blockEnergyLimit)
            return false;

        // TODO: we dont have a minimum gasPrice limit yet
        // for now just pretend its 1
        long energyCost = 0;
        if ((energyCost = calculateGasCost(txStatus)) != -1) {
            return false;
        }

        if (txStatus.transaction.getNrg() < energyCost) {
            return false;
        }

        return true;
    }

    /**
     *
     *
     * @param txStatus status wrapping a possibly valid transaction
     * @param <T> extends ITransaction, some transaction
     * @return {@code amount} representing the minimumGas usage, -1 otherwise to indicate error
     */
    public static <T extends ITransaction> long calculateGasCost(final TransactionStatus<T> txStatus) {
        long gas = VMConstant.ambientTransaction;

        byte[] data;
        if ((data = txStatus.transaction.getData()).length > 0) {
            long nonZero = 0;
            for (int i = 0; i < data.length; i++) {
                if (data[i] != 0)
                    nonZero++;
            }

            // TODO: missing the maximum data check
            gas += nonZero * VMConstant.txdatanonzero;

            long zero = data.length - nonZero;
            gas += zero * VMConstant.txdatazero;
        }
        return gas;
    }
}
