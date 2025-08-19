from decentralized_agents.block import CreditAccount
from typing import Dict, List
import random
import hashlib
import aiorwlock
import hashlib, random, math
from typing import List, Set
import time
import os
import csv
import asyncio

class TestCreditLedger:
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
        # asyncio.create_task(self.auto_stake_loop())


    async def create_account(self, node_id: str, initial_credit: float, initial_staked: float) -> bool:
        async with self.accounts_lock.writer_lock:
            if node_id not in self.accounts:
                self.accounts[node_id] = CreditAccount(node_id=node_id, credit=initial_credit, staked=initial_staked)
                async with self.stakes_lock.writer_lock:
                    self.stakes[node_id] = self.accounts[node_id].staked
                return True
            return False
    

    async def delete_account(self, node_id: str):
        async with self.accounts_lock.writer_lock:
            if node_id in self.accounts:
                del self.accounts[node_id]
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
        async with self.accounts_lock.writer_lock:
            async with self.stakes_lock.writer_lock:
                account = self.accounts.get(node_id)
                if not account or account.credit < amount:
                    return False
                self.stakes[node_id] += amount
                account.credit -= amount
                account.staked += amount
                return True


    async def unstake(self, node_id: str, amount: float) -> bool:
        """Unstake: move 'amount' from staked -> credit."""
        if amount <= 0:
            return True
        async with self.accounts_lock.writer_lock:
            async with self.stakes_lock.writer_lock:
                account = self.accounts.get(node_id)
                if not account or account.staked < amount:
                    return False
                self.stakes[node_id] -= amount
                account.staked -= amount
                account.credit += amount
                return True


    async def reward(self, from_id: str, to_id: str, amount: float) -> bool:
        async with self.accounts_lock.writer_lock:
            from_account = self.accounts.get(from_id)
            to_account = self.accounts.get(to_id)
            if not from_account or not to_account or from_account.credit < amount:
                return False
            from_account.credit -= amount
            to_account.credit += amount
            return True


    async def select_node_by_pos(self, self_node_id: str, seed: str, k = 3) -> List[str]:
        """Select top-k nodes based on their stakes using a pseudo-random selection."""
        if not self.stakes:
            return []

        async with self.stakes_lock.reader_lock:
            node_ids = list(self.stakes.keys())
            if self_node_id in node_ids:
                node_ids.remove(self_node_id)
            weights = [self.stakes[nid] for nid in node_ids]

        total = sum(weights)

        if total == 0:
            return []

        seed_int = int(hashlib.sha256(seed.encode()).hexdigest(), 16)
        rng = random.Random(seed_int)

        return rng.choices(node_ids, weights=weights, k=k)
    
    


    async def select_node_by_pos_no_dup(
        self, 
        self_node_id: str, 
        seed: str, 
        k: int = 3, 
        exclude: Set[str] = None
    ) -> List[str]:
        """按 stake 加权、不放回地选 k 个节点，支持排除指定节点。"""
        if exclude is None:
            exclude = set()

        async with self.stakes_lock.reader_lock:
            node_ids = list(self.stakes.keys())

        # 排除自己和指定节点
        exclude = set(exclude)
        exclude.add(self_node_id)
        node_ids = [nid for nid in node_ids if nid not in exclude]

        # 取对应权重
        async with self.stakes_lock.reader_lock:
            weights = [self.stakes[nid] for nid in node_ids]

        # 过滤掉权重 <= 0 的节点
        pairs = [(nid, w) for nid, w in zip(node_ids, weights) if w > 0]
        if not pairs:
            return []

        node_ids, weights = zip(*pairs)

        # 固定随机数种子
        seed_int = int(hashlib.sha256(seed.encode()).hexdigest(), 16)
        rng = random.Random(seed_int)

        # Efraimidis–Spirakis 算法：每个节点生成一个随机 key，按 key 排序取前 k 个
        scored = [(-math.log(rng.random()) / w, nid) for nid, w in zip(node_ids, weights)]
        scored.sort(key=lambda x: x[0])  # 按 key 升序
        k = min(k, len(scored))

        return [nid for _, nid in scored[:k]]

    async def transfer_nothing(self, from_id: str, to_id: str) -> bool:
        """Transfer nothing from one account to another."""
        decrease_map, increase_map = {}, {}
        for node_id in [from_id, to_id]:
            decrease_map[node_id] = 0
            increase_map[node_id] = 0
        await self._log_duel_action("transfer_nothing", decrease_map, increase_map)
        return True

    async def transfer_all_stake(self, from_id: str, to_id: str) -> bool:
            decrease_map, increase_map = {}, {}
            async with self.accounts_lock.writer_lock:
                async with self.stakes_lock.writer_lock:
                    from_stake = self.stakes.get(from_id, 0.0)
                    if from_stake == 0:
                        return False
                    amount = from_stake
                    self.accounts[from_id].staked -= amount
                    self.stakes[from_id] -= amount
                    decrease_map[from_id] = amount
                self.accounts[to_id].credit += amount
                increase_map[to_id] = amount
            await self._log_duel_action("transfer_all_stake", decrease_map, increase_map)
            return True

    async def judge_transfer(self, majority: List[str], minority: List[str], non_participants: List[str]):
        decrease_map, increase_map = {}, {}
        amount = 0.0
        async with self.accounts_lock.writer_lock:
            async with self.stakes_lock.writer_lock:
                for node_id in minority:
                    dec = self.stakes[node_id] * 0.2
                    amount += dec
                    self.stakes[node_id] -= dec
                    self.accounts[node_id].staked -= dec
                    decrease_map[node_id] = dec
                for node_id in non_participants:
                    dec = self.stakes[node_id] * 0.05
                    amount += dec
                    self.stakes[node_id] -= dec
                    self.accounts[node_id].staked -= dec
                    decrease_map[node_id] = dec
            reward = amount / len(majority) if majority else 0
            for node_id in majority:
                self.accounts[node_id].credit += reward
                increase_map[node_id] = reward
        await self._log_duel_action("judge_transfer", decrease_map, increase_map,
                              details=f"minority={minority}, non_participants={non_participants}, majority={majority}")

    async def judge_transfer_tied(self, participants: List[str], non_participants: List[str]):
        decrease_map, increase_map = {}, {}
        amount = 0.0
        if len(participants) == 0:
            return
        async with self.accounts_lock.writer_lock:
            async with self.stakes_lock.writer_lock:
                for node_id in non_participants:
                    dec = 0
                    amount += dec
                    self.stakes[node_id] -= dec
                    self.accounts[node_id].staked -= dec
                    decrease_map[node_id] = dec
            reward = amount / len(participants)
            for node_id in participants:
                self.accounts[node_id].credit += reward
                increase_map[node_id] = reward
        await self._log_duel_action("judge_transfer_tied", decrease_map, increase_map,
                              details=f"participants={participants}, non_participants={non_participants}")

    async def _log_duel_action(self, action: str, decrease_map: dict, increase_map: dict, details=None):
        ts = time.time()
        decrease_str = ";".join([f"{nid}:{amt:.4f}" for nid, amt in decrease_map.items()])
        increase_str = ";".join([f"{nid}:{amt:.4f}" for nid, amt in increase_map.items()])
        async with self.log_lock:
            with open(self.duel_record_pth, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([ts, action, decrease_str, increase_str, details or ""])

    # async def auto_stake_loop(self):
    #     while True:
    #         await asyncio.sleep(5)
    #         async with self.accounts_lock:
    #             for node_id, account in self.accounts.items():
    #                 if account.credit > account.staked:
    #                     stake_amount = (account.credit - account.staked) / 2
    #                     async with self.stakes_lock:
    #                         self.stakes[node_id] += stake_amount
    #                         account.credit -= stake_amount
    #                         account.staked += stake_amount
                            

