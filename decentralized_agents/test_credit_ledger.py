from decentralized_agents.block import CreditAccount
from typing import Dict, List
import random
import hashlib
import aiorwlock


class TestCreditLedger:
    def __init__(self):
        self.accounts: Dict[str, CreditAccount] = {}
        self.accounts_lock = aiorwlock.RWLock()

        self.stakes: Dict[str, float] = {}  # node_id -> staked amount
        self.stakes_lock = aiorwlock.RWLock()

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
        async with self.stakes_lock.writer_lock:
            async with self.accounts_lock.writer_lock:
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
        async with self.stakes_lock.writer_lock:
            async with self.accounts_lock.writer_lock:
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
                            

