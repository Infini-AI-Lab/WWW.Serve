from typing import Dict, List, Set
import random
import time
import aiorwlock
import math
import os
import csv
import asyncio

from .entities import CreditAccount


class CreditLedger:
    def __init__(self, duel_record_pth: str = None):
        self.accounts: Dict[str, CreditAccount] = {}
        self.accounts_lock = aiorwlock.RWLock()

        self.stakes: Dict[str, float] = {}  # node_id -> staked amount
        self.stakes_lock = aiorwlock.RWLock()

        self.duel_record_pth = duel_record_pth
        self.log_lock = asyncio.Lock()
        if self.duel_record_pth and not os.path.exists(self.duel_record_pth):
            with open(self.duel_record_pth, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp", "action", "decrease", "increase", "details"])


    async def create_account(self, node_id: str, initial_credit: float, initial_staked: float) -> bool:
        async with self.accounts_lock.writer_lock:
            if node_id not in self.accounts:
                self.accounts[node_id] = CreditAccount(node_id=node_id, credit=initial_credit, staked=initial_staked)
            else:
                return True
        async with self.stakes_lock.writer_lock:
            self.stakes[node_id] = self.accounts[node_id].staked
        return True


    async def delete_account(self, node_id: str):
        async with self.accounts_lock.writer_lock:
            if node_id in self.accounts:
                del self.accounts[node_id]
            else:
                return
        async with self.stakes_lock.writer_lock:
            del self.stakes[node_id]


    async def get_account_credit(self, node_id: str) -> float:
        async with self.accounts_lock.reader_lock:
            account = self.accounts.get(node_id)
            return account.credit if account else 0.0


    async def get_stake(self, node_id: str) -> float:
        """Return the current stake of a node."""
        async with self.stakes_lock.reader_lock:
            return self.stakes.get(node_id, 0.0)


    async def get_all_stakes(self) -> Dict[str, float]:
        async with self.stakes_lock.reader_lock:
            return self.stakes.copy()


    async def stake(self, node_id: str, amount: float) -> bool:
        async with self.accounts_lock.reader_lock:
            account = self.accounts.get(node_id)
            if not account or account.credit < amount:
                return False
        async with self.stakes_lock.writer_lock:
            self.stakes[node_id] += amount
        async with self.accounts_lock.writer_lock:
            account.credit -= amount
            account.staked += amount
        return True


    async def unstake(self, node_id: str, amount: float) -> bool:
        """Unstake: move 'amount' from staked -> credit."""
        if amount <= 0:
            return True
        async with self.accounts_lock.reader_lock:
            account = self.accounts.get(node_id)
            if not account or account.staked < amount:
                return False
        async with self.stakes_lock.writer_lock:
            self.stakes[node_id] -= amount
        async with self.accounts_lock.writer_lock:
            account.staked -= amount
            account.credit += amount
        return True


    async def reward(self, from_id: str, to_id: str, amount: float) -> bool:
        async with self.accounts_lock.reader_lock:
            from_account = self.accounts.get(from_id)
            to_account = self.accounts.get(to_id)
            if not from_account or not to_account or from_account.credit < amount:
                return False
        async with self.accounts_lock.writer_lock:
            from_account.credit -= amount
            to_account.credit += amount

        await self._log_duel_action(
            "reward",
            {from_id: amount},
            {to_id: amount},
            # details=f"{from_id} -> {to_id}, amount={amount}"
        )

        return True


    async def reward_judge(self, from_id: str, to_id: str, amount: float) -> bool:
        async with self.accounts_lock.reader_lock:
            from_account = self.accounts.get(from_id)
            to_account = self.accounts.get(to_id)
            if not from_account or not to_account or from_account.credit < amount:
                return False
        async with self.accounts_lock.writer_lock:
            from_account.credit -= amount
            to_account.credit += amount

        await self._log_duel_action(
            "reward_judge",
            {from_id: amount},
            {to_id: amount},
        )

        return True


    async def select_node_by_pos(
        self,
        exclude_nodes: Set[str],
        k: int = 3
    ) -> List[str]:
        """PoS selection (without replacement)."""
        if not self.stakes:
            return []

        async with self.stakes_lock.reader_lock:
            node_ids = [nid for nid in self.stakes.keys() if nid not in exclude_nodes]
            weights = [self.stakes[nid] for nid in node_ids]

        pairs = [(nid, w) for nid, w in zip(node_ids, weights) if w > 0]
        if not pairs:
            return []

        node_ids, weights = zip(*pairs)
        # Efraimidis–Spirakis
        scored = [(-math.log(random.random()) / w, nid) for nid, w in zip(node_ids, weights)]
        scored.sort(key=lambda x: x[0])
        k = min(k, len(scored))

        return [nid for _, nid in scored[:k]]


    async def transfer_nothing(self, from_id: str, to_id: str) -> bool:
        """Transfer nothing from one account to another."""
        pass


    async def transfer_stake(self, from_id: str, to_id: str, gamma: float = 1.0) -> bool:
        decrease_map, increase_map = {}, {}

        async with self.accounts_lock.writer_lock, self.stakes_lock.writer_lock:
            from_stake = self.stakes.get(from_id, 0.0)
            if from_stake == 0:
                return False

            transfer_amount = gamma * from_stake

            self.stakes[from_id] -= transfer_amount
            self.accounts[from_id].staked -= transfer_amount
            decrease_map[from_id] = transfer_amount

            self.accounts[to_id].credit += transfer_amount
            increase_map[to_id] = transfer_amount

        await self._log_duel_action(
            "transfer_stake",
            decrease_map,
            increase_map,
        )
        return True



    async def judge_transfer(self, majority: List[str], minority: List[str], non_participants: List[str]):
        decrease_map, increase_map = {}, {}
        amount = 0.0

        async with self.accounts_lock.writer_lock, self.stakes_lock.writer_lock:
            def deduct(node_ids, factor):
                nonlocal amount
                for node_id in node_ids:
                    dec = self.stakes[node_id] * factor
                    self.stakes[node_id] -= dec
                    self.accounts[node_id].staked -= dec
                    decrease_map[node_id] = dec
                    amount += dec

            deduct(minority, 0.2)
            deduct(non_participants, 0.05)

            reward = amount / len(majority) if majority else 0
            for node_id in majority:
                self.accounts[node_id].credit += reward
                increase_map[node_id] = reward

        await self._log_duel_action(
            "judge_transfer",
            decrease_map,
            increase_map,
            details=f"minority={minority}, non_participants={non_participants}, majority={majority}"
        )



    async def judge_transfer_tied(self, participants: List[str], non_participants: List[str]):
        if not participants:
            return

        decrease_map = {}
        increase_map = {}
        amount = 0.0

        async with self.accounts_lock.writer_lock, self.stakes_lock.writer_lock:
            for node_id in non_participants:
                dec = 0  # TODO: replace with actual calculation if needed
                self.stakes[node_id] -= dec
                self.accounts[node_id].staked -= dec
                decrease_map[node_id] = dec
                amount += dec

            reward = amount / len(participants)
            for node_id in participants:
                self.accounts[node_id].credit += reward
                increase_map[node_id] = reward

        await self._log_duel_action(
            "judge_transfer_tied",
            decrease_map,
            increase_map,
            details=f"participants={participants}, non_participants={non_participants}"
        )



    async def _log_duel_action(self, action: str, decrease_map: dict, increase_map: dict, details=None):
        ts = time.time()
        decrease_str = ";".join([f"{nid}:{amt:.4f}" for nid, amt in decrease_map.items()])
        increase_str = ";".join([f"{nid}:{amt:.4f}" for nid, amt in increase_map.items()])
        async with self.log_lock:
            with open(self.duel_record_pth, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([ts, action, decrease_str, increase_str, details or ""])