package org.aion.zero.impl.tx;

import org.aion.base.type.ITransaction;
import org.aion.base.util.ByteUtil;

import java.math.BigInteger;
import java.util.*;

/**
 * Organizes transactions based on optimal price
 */
public class AccountTxList<T extends ITransaction> {

    /**
     * Single static instance of transaction status, used to represent
     * that an error occured in put (was not successful), we may want to formalize
     * this later but this is an efficient way of representing an error state
     */
    public static final TransactionStatus ERROR_STATUS = new TransactionStatus<>();

    /**
     * Maintains a set of transactions ordered based on their heap, these transactions
     * should reference the same transactions as those maintained in transactionMap
     */
    private Queue<TransactionStatus<T>> nonceHeap;

    /**
     * Primary data structure, maintaining a mapping from Nonce -> Transaction
     */
    private HashMap<Long, TransactionStatus<T>> transactionMap;

    /**
     * Cached list of transactions that are already sorted, this is cached
     * until our assumptions about the internal structure are invalidated
     */
    private List<T> sortedTransactions;

    /**
     * Represents the total amount of gas used by this account according to the
     * currently added transactions list, this is used to determine whether the
     * account has sufficient balance to execute
     */
    private long totalGasUsage;

    /**
     * Represents the last time this account was updated, this in turn can be
     * used to determined the amount of "activity" this account has
     */
    private volatile long lastTouched;

    /**
     * Represents the earliest point in the list of transactions that contains
     * a gap, if this is set to -1, it indicates that the whole structure
     * contains no gaps.
     *
     * Specifically, the earliest gap target points to the nonce of the last
     * transaction that is part the list extending from the first element
     * known to us.
     *
     * For a pending list, this should refer to the latest element in
     * the list. Note that this is not the size of the list.
     *
     * -1 indicates the list is empty
     */
    private long earliestGapTarget = -1;

    private final boolean sequential;

    public AccountTxList(boolean sequential) {
        this.nonceHeap = new PriorityQueue<>(Comparator.comparingLong((t) -> t.nonce));
        this.transactionMap = new HashMap<>();
        this.sequential = sequential;
    }

    // called when we add a new element
    private boolean updateGapTarget(TransactionStatus<T> newTx) {
        // we may hit this case when the array becomes empty
        // this can occur when we remove all elements or when we
        // first initialize the array
        if (this.earliestGapTarget == -1) {
            this.earliestGapTarget = newTx.nonce;
            return true;
        }
        // this indicates there was previously no gaps
        long target = earliestGapTarget;
        if (newTx.nonce <= target && newTx.nonce != (target + 1))
            return false;
        this.earliestGapTarget = this.earliestGapTarget + 1;
        return true;
    }

    // called when we "update" the map, a possiblity is that the element the gap
    // targets is deleted (for example when the array becomes empty) and must
    // be updated to point to the new latest
    // TODO: logic is faulty atm, needs to be fixed
    private boolean updateGapTarget() {
        if (this.nonceHeap.isEmpty() && this.transactionMap.isEmpty()) {
            this.earliestGapTarget = -1;
            return true;
        }

        if (this.transactionMap.get(this.earliestGapTarget) == null) {
            long earliest = this.nonceHeap.peek().nonce;
            // we can get away with simply decrementing because we call this update function
            // everytime we remove an element, therefore assuming all elements up to the gap
            // are sequential (and adjacent) we can simply decrement
            if (earliest < this.earliestGapTarget) {
                this.earliestGapTarget = this.earliestGapTarget - 1;
                return true;
            } else {
                // otherwise the function was an update
                this.earliestGapTarget = this.nonceHeap.peek().nonce;
                return true;
            }
        }
        return false;
    }

    public synchronized TransactionStatus<T> add(TransactionStatus<T> transaction) {
        Long nonce = transaction.nonce;

        TransactionStatus<T> tx;
        if ((tx = transactionMap.get(nonce)) != null) {
            // if a transaction with the same nonce arrives, check
            // whether we should update the previous transaction
            if (tx.transaction.getNrgPrice() < transaction.transaction.getNrgPrice()) {
                return ERROR_STATUS;
            }

            // remove the transaction from the nonceHeap and map
            // to prepare for an add
            transactionMap.remove(nonce);
            nonceHeap.remove(tx);
        }

        // invalidate the cache, we've possibly altered the state
        this.sortedTransactions = null;

        // update when this account was last touched
        if (transaction.timestamp > lastTouched)
            lastTouched = transaction.timestamp;

        // we definitely want to add this transaction, either its new or
        // it overrides some previously held nonce
        nonceHeap.add(transaction);
        TransactionStatus<T> old = transactionMap.put(nonce, transaction);
        updateGapTarget(transaction);
        return old;
    }

    public synchronized boolean remove(TransactionStatus<T> transaction) {
        Long nonce = transaction.nonce;

        if (!this.transactionMap.containsKey(nonce)) {
            return false;
        }

        // otherwise remove
        TransactionStatus<T> status = this.transactionMap.remove(nonce);
        this.nonceHeap.remove(status);

        if (sequential) {
            Iterator<TransactionStatus<T>> it = this.nonceHeap.iterator();
            while (it.hasNext()) {
                TransactionStatus<T> item = it.next();
                if (item.nonce > nonce) {
                    it.remove();
                    this.transactionMap.remove(item.nonce);
                }
            }
        }

        updateGapTarget();
        return true;
    }

    public synchronized List<T> getPending() {
        if (this.sortedTransactions == null) {
            if (this.nonceHeap.isEmpty())
                return Collections.emptyList();

            List<T> pendingList = new ArrayList<>();
            Iterator<TransactionStatus<T>> it = this.nonceHeap.iterator();

            // iterate until we find a gap, only use the list of transactions
            // that have sequential nonces
            TransactionStatus<T> prev = it.next();
            pendingList.add(prev.transaction);
            while(it.hasNext()) {
                TransactionStatus<T> t = it.next();
                if (t.nonce != (prev.nonce + 1))
                    break;
                pendingList.add(t.transaction);
            }
            this.sortedTransactions = pendingList;
        }
        return this.sortedTransactions;
    }

    /**
     * Updates list so that all transactions with a nonce lower than the
     * specified nonce value is removed
     */
    public synchronized void update(long nonce) {
        TransactionStatus<T> localMinTx = this.nonceHeap.peek();
        if (localMinTx == null)
            return;

        long localMinNonce = localMinTx.nonce;

        if (nonce <= localMinNonce)
            return;

        TransactionStatus<T> minTx;
        while((minTx = this.nonceHeap.peek()) != null) {
            if (minTx.nonce < nonce) {
                this.nonceHeap.remove();
                boolean success = this.transactionMap.remove(minTx.nonce, minTx);
                if (!success)
                    throw new RuntimeException("desync between transactionMap and heap");
            }
        }
        updateGapTarget();
    }

    public synchronized boolean containsNonce(TransactionStatus<T> transaction) {
        return this.transactionMap.containsKey(transaction.nonce);
    }

    /**
     * Get all transactions stored in this list, regardless of gaps
     *
     * TODO: cache this later
     * @return
     */
    public synchronized List<TransactionStatus<T>> getAll() {
        List<TransactionStatus<T>> statuses = new ArrayList<>();
        statuses.addAll(this.nonceHeap);
        return statuses;
    }

    public synchronized List<TransactionStatus<T>> removeBelowNonce(final long nonce) {
        List<TransactionStatus<T>> removed = new ArrayList<>();
        while(!this.nonceHeap.isEmpty()) {
            TransactionStatus<T> tx = this.nonceHeap.peek();
            if (tx.nonce < nonce) {
                this.nonceHeap.remove();
                this.transactionMap.remove(tx.nonce);
                removed.add(tx);
            }
        }
        updateGapTarget();
        return removed;
    }

    private List<TransactionStatus<T>> removeAboveNonce(final long nonce) {
        List<TransactionStatus<T>> removed = new ArrayList<>();

        Iterator<TransactionStatus<T>> it = this.nonceHeap.iterator();
        while(it.hasNext()) {
            TransactionStatus<T> status = it.next();
            if (status.nonce > nonce) {
                it.remove();
                this.transactionMap.remove(status.nonce);
                removed.add(status);
            }
        }
        return removed;
    }

    // TODO: add strict
    public synchronized Map.Entry<List<TransactionStatus<T>>, List<TransactionStatus<T>>> removeUpdateState(
            final BigInteger balance,
            final long nrgLimit) {

        long minNonce = Long.MAX_VALUE;
        ArrayList<TransactionStatus<T>> removed = new ArrayList<>();
        for (TransactionStatus<T> status : this.transactionMap.values()) {
            if (status.reqBalance.compareTo(balance) > 0 ||
                    status.transaction.getNrg() > nrgLimit) {
                removed.add(status);
                // remove from respective maps
                this.nonceHeap.remove(status);
                this.transactionMap.remove(status.nonce);

                if (minNonce > status.nonce)
                    minNonce = status.nonce;
            }
        }

        List<TransactionStatus<T>> invalid = Collections.emptyList();
        if (sequential) {
            invalid = removeAboveNonce(minNonce);
        }

        updateGapTarget();
        return Map.entry(removed, invalid);
    }

    public long getLastTouched() {
        return this.lastTouched;
    }
}
