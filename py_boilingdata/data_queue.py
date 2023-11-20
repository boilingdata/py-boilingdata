class DataQueue:
    def __init__(self, requestId, callback, fut=None):
        self.requestId = requestId
        self.callback = callback
        self.fut = fut
        self.data = []
        self.batchCounters = dict()
        self.parts_done = False
        self.have_new_messages = False
        self.is_deleted = False
        return

    def _have_all_parts(self):
        if self.is_deleted:
            raise Exception("Deleted queue!")
        if not self.have_new_messages:
            return self.parts_done
        if self.parts_done == True:
            return True
        msg = self.data[0]
        totalBatches = msg.get("totalBatches")
        totalSubBatches = msg.get("totalSubBatches")
        totalSplitSerials = msg.get("totalSplitSerials")
        # batchSerial / totalBatches
        # subBatchSerial / totalSubBatches
        # splitSerial / totalSplitSerials
        for msg in self.data:
            data = msg.get("data")
            batchSerial = msg.get("batchSerial")
            subBatchSerial = msg.get("subBatchSerial")
            splitSerial = msg.get("splitSerial")
            if batchSerial not in self.batchCounters:
                self.batchCounters[batchSerial] = {
                    "subBatches": dict(),
                    "totalSubBatches": totalSubBatches,
                }
            batchCounter = self.batchCounters.get(batchSerial)
            if subBatchSerial:
                if subBatchSerial not in batchCounter["subBatches"]:
                    batchCounter["subBatches"][subBatchSerial] = {
                        "splits": dict(),
                        "totalSplits": totalSplitSerials,
                    }
                subBatch = batchCounter["subBatches"][subBatchSerial]
                if splitSerial:
                    if splitSerial not in subBatch["splits"]:
                        subBatch["splits"][splitSerial] = {"data": data}
                else:
                    subBatch["data"] = data
            else:
                batchCounter["data"] = data
        # print(
        #     f"RequestId {self.requestId}:\n\t"
        #     + f"Batches {len(self.batchCounters)}/{totalBatches}"
        # )
        self.have_new_messages = False
        if len(self.batchCounters) < totalBatches:
            return False
        for value in self.batchCounters.values():
            totalSubBatches = value.get("totalSubBatches")
            if totalSubBatches:
                # print(f"\tsubBatches: {len(value.get('subBatches'))}/{totalSubBatches}")
                if len(value.get("subBatches")) < totalSubBatches:
                    return False
                for value in value.get("subBatches").values():
                    totalSplits = value.get("totalSplits")
                    if totalSplits:
                        # print(f"\tsplits: {len(value.get('splits'))}/{totalSplits}")
                        if len(value.get("splits")) < totalSplits:
                            return False
        self.parts_done = True
        return True

    def _compile(self):
        if self.is_deleted:
            raise Exception("Deleted queue!")
        data = []
        # We have all the data, compile it in order
        for key1, value1 in sorted(self.batchCounters.items()):
            if "data" in value1:
                data.extend(value1["data"])
            if "subBatches" in value1:
                for key2, value2 in sorted(value1.get("subBatches").items()):
                    if "data" in value2:
                        data.extend(value2["data"])
                    if "splits" in value2:
                        for key3, value3 in sorted(value2.get("splits").items()):
                            if "data" in value3:
                                data.extend(value3["data"])
        return data

    def _notify(self):
        if self.is_deleted:
            raise Exception("Deleted queue!")
        data = self._compile()
        if self.callback:
            self.callback({"data": data, "requestId": self.requestId})
        if self.fut:
            self.fut.set_result(data)

    ##
    ## public
    ##

    def push(self, msg):
        if self.is_deleted:
            raise Exception("Deleted queue!")
        self.data.append(msg)
        self.have_new_messages = True
        if self._have_all_parts():
            self._notify()  # callback and future
        return

    # Prefer the callback method
    def get_data(self):
        if self.is_deleted:
            raise Exception("Deleted queue!")
        if self.is_done():
            return self._compile()
        return None

    def is_done(self):
        if self.is_deleted:
            raise Exception("Deleted queue!")
        return self.parts_done == True or self._have_all_parts()

    def delete(self):
        self.is_deleted = True
        self.data = None
        del self.batchCounters
        return
