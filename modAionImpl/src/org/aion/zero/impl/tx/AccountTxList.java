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

    private final boolean sequential;

    public AccountTxList(boolean sequential) {
        this.nonceHeap = new PriorityQueue<>(Comparator.comparingLong((t) -> t.nonce));
        this.transactionMap = new HashMap<>();
        this.sequential = sequential;
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
        return removed;
    }

    /**
     * Removes all elements above a certain nonce, used in sequential lists where
     * we must guarantee there are no gaps in the list
     *
     * @param nonce
     * @return
     */
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


    /**
     * Removes elements if they fail the following criteria
     *
     * @param balance
     * @param nrgLimit
     * @return
     */
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

        return Map.entry(removed, invalid);
    }

    public long getLastTouched() {
        return this.lastTouched;
    }

    /**
     * Returns the sequential list of blocks and removes them from the list
     *
     * @return {@code list} of blocks starting from the given nonce
     */
    public synchronized List<TransactionStatus<T>> takeSequential(long nonce) {
        if (this.nonceHeap.isEmpty() || this.nonceHeap.peek().nonce > nonce)
            return null;

        List<TransactionStatus<T>> sequential = new ArrayList<>();

        long previousNonce = 0;
        // initial setup
        Iterator<TransactionStatus<T>> it = this.nonceHeap.iterator();
        TransactionStatus<T> firstNext = it.next();
        previousNonce = firstNext.nonce;
        sequential.add(firstNext);
        while (it.hasNext()) {
            TransactionStatus<T> next = it.next();

            // if we have a gap, break immediately
            if (next.nonce != previousNonce + 1)
                break;

            // otherwise its sequential, add to list
            sequential.add(next);

            // remove from our list
            it.remove();
            this.transactionMap.remove(next.nonce);
            previousNonce++;
        }
        return sequential;
    }

    public synchronized boolean isEmpty() {
        return this.nonceHeap.isEmpty();
    }
}
