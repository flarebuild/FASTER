﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private OperationStatus TryFindAndEphemeralLockAuxiliaryRecord<Input, Output, Context, FasterSession>(
                FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                LockType lockType, long prevHighestKeyHashAddress = Constants.kInvalidAddress)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (UseReadCache && FindInReadCache(ref key, ref stackCtx, 
                                                untilAddress: (prevHighestKeyHashAddress & Constants.kReadCacheBitMask) != 0 ? prevHighestKeyHashAddress : Constants.kInvalidAddress))
                return TryLockInMemoryRecord<Input, Output, Context, FasterSession>(fasterSession, ref stackCtx, lockType);

            if (LockTable.IsActive && !fasterSession.DisableEphemeralLocking && !LockTable.TryLockEphemeral(ref key, stackCtx.hei.hash, lockType, out stackCtx.recSrc.HasLockTableLock))
                return OperationStatus.RETRY_LATER;
            return OperationStatus.SUCCESS;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private OperationStatus TryFindAndEphemeralLockRecord<Input, Output, Context, FasterSession>(
                FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                LockType lockType, long prevHighestKeyHashAddress = Constants.kInvalidAddress)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var internalStatus = TryFindAndEphemeralLockAuxiliaryRecord<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, lockType, prevHighestKeyHashAddress);
            if (stackCtx.recSrc.HasSrc)
                return internalStatus;

            if (!TryFindRecordInMainLog(ref key, ref stackCtx, minOffset: hlog.HeadAddress, waitForTentative: true))
                return OperationStatus.SUCCESS;
            return TryLockInMemoryRecord<Input, Output, Context, FasterSession>(fasterSession, ref stackCtx, lockType);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static OperationStatus TryLockInMemoryRecord<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref OperationStackContext<Key, Value> stackCtx, LockType lockType) where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            ref var recordInfo = ref stackCtx.recSrc.GetSrcRecordInfo();
            var ok = lockType == LockType.Shared
                ? fasterSession.TryLockEphemeralShared(ref recordInfo)
                : fasterSession.TryLockEphemeralExclusive(ref recordInfo);
            if (!ok)
                return OperationStatus.RETRY_LATER;
            stackCtx.recSrc.HasInMemoryLock = !fasterSession.DisableEphemeralLocking;
            return OperationStatus.SUCCESS;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static bool TryEphemeralXLock<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref RecordSource<Key, Value> recSrc, ref RecordInfo recordInfo, out OperationStatus status)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            status = OperationStatus.SUCCESS;
            if (fasterSession.DisableEphemeralLocking)
            {
                Debug.Assert(!fasterSession.IsManualLocking || recordInfo.IsLockedExclusive, $"Attempting to use a non-XLocked key in a Manual Locking context (requesting XLock): XLocked {recordInfo.IsLockedExclusive}, Slocked {recordInfo.NumLockedShared}");
                return true;
            }

            // A failed lockOp means this is an intermediate record, e.g. Tentative or Sealed, or we exhausted the spin count. All these must RETRY_LATER.
            if (!fasterSession.TryLockEphemeralExclusive(ref recordInfo))
                status = OperationStatus.RETRY_LATER;
            else if (!IsRecordValid(recordInfo, out status))
                fasterSession.UnlockEphemeralExclusive(ref recordInfo);
            else
                recSrc.HasInMemoryLock = true;
            return recSrc.HasInMemoryLock;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static bool TryEphemeralSLock<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref RecordSource<Key, Value> recSrc, ref RecordInfo recordInfo, out OperationStatus status)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            status = OperationStatus.SUCCESS;
            if (fasterSession.DisableEphemeralLocking)
            {
                Debug.Assert(!fasterSession.IsManualLocking || recordInfo.IsLocked, $"Attempting to use a non-Locked (S or X) key in a Manual Locking context (requesting SLock): XLocked {recordInfo.IsLockedExclusive}, Slocked {recordInfo.NumLockedShared}");
                return true;
            }

            // A failed lockOp means this is an intermediate record, e.g. Tentative or Sealed, or we exhausted the spin count. All these must RETRY_LATER.
            if (!fasterSession.TryLockEphemeralShared(ref recordInfo))
                status = OperationStatus.RETRY_LATER;
            else if (!IsRecordValid(recordInfo, out status))
                fasterSession.TryUnlockEphemeralShared(ref recordInfo);
            else
                recSrc.HasInMemoryLock = true;
            return recSrc.HasInMemoryLock;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void EphemeralSUnlock<Input, Output, Context, FasterSession>(FasterSession fasterSession, FasterExecutionContext<Input, Output, Context> currentCtx,
                                                                     ref PendingContext<Input, Output, Context> pendingContext,
                                                                     ref Key key, ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo recordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (!stackCtx.recSrc.HasLock)
                return;

            // This is called on recovery from Pending Read, in which case we may have a LockTable lock. First try to unlock the in-memory record.
            // If on recovery in either immediate or pending read, then we may have evicted the source address or it may have been Read/CopyToTail.
            // So we fall through on a failed unlock or non-InMemory lock.
            if (!stackCtx.recSrc.HasInMemoryLock || stackCtx.recSrc.LogicalAddress < stackCtx.recSrc.Log.HeadAddress || !recordInfo.TryUnlockShared())
            {
                // Updaters (Upsert, RMW, Delete) XLock records. Readers do not, including anything calling InternalTryCopyToTail. This means the record may
                // be transferred from the readcache to the main log (or even to the LockTable, if the record was in the (SafeHeadAddress, ClosedUntilAddress)
                // interval when a Read started).

                if (stackCtx.recSrc.HasLockTableLock && LockTable.Unlock(ref key, stackCtx.hei.hash, LockType.Shared))
                    return;

                // If the record dived below HeadAddress, we must wait for it to enter the lock table before unlocking; InternalLock does this (and starts 
                // by searching the in-memory space first, which is good because the record may have been transferred).
                // If RecordInfo unlock fails, the locks were transferred to another recordInfo; do InternalLock to chase the key through the full process.
                OperationStatus status;
                do
                {
                    status = InternalLock(ref key, new(LockOperationType.Unlock, LockType.Shared), out _);
                } while (HandleImmediateRetryStatus(status, currentCtx, currentCtx, fasterSession, ref pendingContext));
            }

            stackCtx.recSrc.HasInMemoryLock = false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EphemeralXUnlockAfterUpdate<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key,
                ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (fasterSession.DisableEphemeralLocking)
            {
                Debug.Assert(!stackCtx.recSrc.HasLock, "HasLock should only be true if we are doing ephemeral locking");
                return;
            }

            // Unlock exclusive locks, if any. Exclusive locks are different from shared locks, in that Shared locks can be transferred
            // (due to CopyToTail or ReadCache) while the lock is held. Exclusive locks pin the lock in place:
            //   - The owning thread ensures that no epoch refresh is done, so there is no eviction if it is in memory
            //   - Other threads will attempt (and fail) to lock it in memory or in the locktable, until we release it.
            //     - This means there can be no transfer *from* the locktable while the XLock is held
            if (stackCtx.recSrc.HasInMemoryLock)
            {
                // This unlocks the source (old) record; the new record may already be operated on by other threads, which is fine.
                if (stackCtx.recSrc.LogicalAddress >= stackCtx.recSrc.Log.HeadAddress)
                {
                    // The record is now Invalid or Sealed, but we have to unlock it so any threads waiting on it can continue.
                    fasterSession.UnlockEphemeralExclusive(ref srcRecordInfo);
                }
                else
                {
                    // We must always wait until the lock table entry is in place; it will be orphaned because we've transferred the record,
                    // so we must remove it from the LockTable.
                    SpinWaitUntilRecordIsClosed(ref key, stackCtx.hei.hash, stackCtx.recSrc.LogicalAddress, stackCtx.recSrc.Log);
                    LockTable.Remove(ref key, stackCtx.hei.hash);
                }
                stackCtx.recSrc.HasInMemoryLock = false;
                return;
            }

            if (stackCtx.recSrc.HasLockTableLock)
            {
                LockTable.Unlock(ref key, stackCtx.hei.hash, LockType.Exclusive);
                stackCtx.recSrc.HasLockTableLock = false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EphemeralXUnlockAndAbandonUpdate<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref RecordSource<Key, Value> recSrc, ref RecordInfo srcRecordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            if (!fasterSession.DisableEphemeralLocking)
                fasterSession.UnlockEphemeralExclusive(ref srcRecordInfo);
            recSrc.ClearSrc();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CompleteTwoPhaseUpdate<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                        ref RecordInfo srcRecordInfo, ref RecordInfo newRecordInfo, out OperationStatus status)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            // We don't check for ephemeral xlocking here; we know we had that lock, but we don't need to actually lock the new record because
            // we know this is the last step and we are going to unlock it immediately; it is protected until we remove the Tentative bit.

            if (fasterSession.IsManualLocking)
            {
                // For manual locking, we should already have made sure there is an XLock for this, and must preserve it on the new record.
                // If we do not have an in-memory source there should be a LockTable entry; transfer from it (which will remove it from the LockTable).
                // Otherwise (we do have an in-memory source) just set the bit directly; we'll mark and clear the IM source below.
                bool transferred = false;
                if (!stackCtx.recSrc.HasInMemorySrc)
                {
                    bool found = this.LockTable.TryGet(ref key, stackCtx.hei.hash, out var ltriLT);
                    Debug.Assert(found && ltriLT.IsLocked && !ltriLT.Tentative, "Error--non-InMemorySrc expected to find a non-tentative locked locktable entry");

                    transferred = LockTable.IsActive && LockTable.TransferToLogRecord(ref key, stackCtx.hei.hash, ref newRecordInfo);
                    Debug.Assert(transferred, "ManualLocking Non-InMemory source should find a LockTable entry to transfer locks from in CompleteTwoPhaseUpdate");
                }
#if DEBUG
                if (this.LockTable.TryGet(ref key, stackCtx.hei.hash, out var ltri))
                {
                    // If !recSrc.HasInMemorySrc, then we just did a LockTable transfer to an existing tentative log record, and that tentative record should have
                    // prevented anyone from making a non-tentative LockTable entry. If we recSrc.HasInMemorySrc, then there should never be LockTable entry.
                    Debug.Assert(!ltri.IsLocked || ltri.Tentative, $"Error--existing non-tentative LT entry in CompleteTwoPhaseUpdate transfer; HasInMemSrc = {stackCtx.recSrc.HasInMemorySrc}");
                }
#endif
                if (!transferred)
                    newRecordInfo.InitializeLockExclusive();
            }
            else if ((LockTable.IsActive && !LockTable.CompleteTwoPhaseUpdate(ref key, stackCtx.hei.hash))
                    || (UseReadCache && !ReadCacheCompleteTwoPhaseUpdate(ref key, ref stackCtx.hei)))
            {
                // A permanent LockTable entry or a ReadCache entry with a lock was added before we inserted the tentative record, so we must invalidate the new record and retry.
                // We cannot reuse the allocation because it's in the hash chain. // TODO consider eliding similar to InternalDelete
                stackCtx.SetNewRecordInvalidAtomic(ref newRecordInfo);
                status = OperationStatus.RETRY_LATER;
                return false;
            }

            status = OperationStatus.SUCCESS;
            stackCtx.recSrc.MarkSourceRecordAfterSuccessfulCopyUpdate<Input, Output, Context, FasterSession>(fasterSession, ref srcRecordInfo);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CompleteTwoPhaseCopyToTail<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                        ref RecordInfo srcRecordInfo, ref RecordInfo newRecordInfo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            // Transfer locks; these will always be read locks and include the caller's read lock if we've not disabled ephemeral locking.
            bool success = true;
            if (stackCtx.recSrc.HasInMemorySrc)
            {
                stackCtx.recSrc.AssertInMemorySourceWasNotEvicted();

                // Unlock the ephemeral lock here; we mark the source, so we *know* we will have an invalid unlock on srcRecordInfo and would have to chase
                // through InternalLock to unlock it, so we save the time by not transferring our ephemeral lock. 'Tentative' still protects the new record.
                success = newRecordInfo.CopyReadLocksFromAndMarkSourceAtomic(ref srcRecordInfo, allowXLock: fasterSession.IsManualLocking,
                                                                    seal: stackCtx.recSrc.HasMainLogSrc, removeEphemeralLock: stackCtx.recSrc.HasInMemoryLock);
                if (success)
                    stackCtx.recSrc.HasInMemoryLock = false;
            }
            else
            {
                if (fasterSession.IsManualLocking)
                {
                    // For manual locking, we should already have made sure there is at least an SLock for this; since there is no HasInMemorySrc, it is in the Lock Table.
                    if (LockTable.IsActive)
                    {
                        if (!LockTable.TransferToLogRecord(ref key, stackCtx.hei.hash, ref newRecordInfo))
                            Debug.Fail("ManualLocking Non-InMemory source should find a LockTable entry to transfer locks from in CompleteTwoPhaseCopyToTail");
                    }
                }
                else
                {
                    // XLocks are not allowed here in the ephemeral section, because another thread owns them (for operations that end up here, ephemeral locking only takes a read lock).
                    var lt_success = !LockTable.IsActive || LockTable.CompleteTwoPhaseCopyToTail(ref key, stackCtx.hei.hash, ref newRecordInfo, allowXLock: fasterSession.IsManualLocking,
                                                                                          removeEphemeralLock: stackCtx.recSrc.HasLockTableLock);

                    // We must check readcache even if we don't have an in-memory source, because another thread may have transferred from the LockTable to the ReadCache.
                    var rc_success = !UseReadCache || ReadCacheCompleteTwoPhaseCopyToTail(ref key, ref stackCtx.hei, ref newRecordInfo, allowXLock: fasterSession.IsManualLocking,
                                                                                    removeEphemeralLock: stackCtx.recSrc.HasLockTableLock);     // HasLockTableLock because we checked HasInMemorySrc above
                    
                    // We don't have to worry about checking the main log, because a CAS by another thread would fail due to our Tentative record being there
                    // for both mainhash entry (CAS compares to entry.word) and readcache (splice CAS compares to LowestLogicalAddress).
                    success = lt_success && rc_success;
                    Debug.Assert(success, "Expected success releasing LockTable lock in Complete2pCTT");

                    if (success)
                        stackCtx.recSrc.HasLockTableLock = false;
                }
            }
            return success;
        }
    }
}
