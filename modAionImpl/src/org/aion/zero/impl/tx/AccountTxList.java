package org.aion.zero.impl.tx;

import org.aion.base.type.ITransaction;
import org.aion.base.util.ByteUtil;

import java.util.*;

/**
 * Organizes transactions based on optimal price
 */
public class AccountTxList<T extends ITransaction> {

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
     */
    private long earliestGapTarget;

    public AccountTxList() {
        this.nonceHeap = new PriorityQueue<>(Comparator.comparingLong((t) -> t.nonce));
        this.transactionMap = new HashMap<>();
    }

    private boolean updateGapTarget(TransactionStatus<T> newTx) {
        // this indicates there was previously no gaps
        long target = earliestGapTarget;
        if (newTx.nonce <= target && newTx.nonce != (target + 1))
            return false;
        this.earliestGapTarget = this.earliestGapTarget + 1;
        return true;
    }

    public synchronized boolean add(TransactionStatus<T> transaction) {
        Long nonce = transaction.nonce;

        TransactionStatus<T> tx;
        if ((tx = transactionMap.get(nonce)) != null) {
            // if a transaction with the same nonce arrives, check
            // whether we should update the previous transaction
            if (tx.transaction.getNrgPrice() < transaction.transaction.getNrgPrice()) {
                return false;
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
        transactionMap.put(nonce, transaction);
        updateGapTarget(tx);
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

    public long getLastTouched() {
        return this.lastTouched;
    }
}
