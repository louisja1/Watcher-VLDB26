# from table import tables
from enum import Enum
import copy

class TupleUpdateType(Enum):
    INSERT = 0
    DELETE = 1

    @staticmethod
    def from_str(_str):
        if _str in [0, "I"]:
            return TupleUpdateType.INSERT
        elif _str in [1, "D"]:
            return TupleUpdateType.DELETE
        else:
            raise NotImplementedError


class RangeSelection:
    def __init__(self, table_name, lb, ub):
        self.table_name = table_name
        self.lb = lb if lb != "" else None
        self.ub = ub if ub != "" else None

    def update_by(self, _lb, _ub):
        if self.lb is None or self.lb < _lb:
            self.lb = _lb
        if self.ub is None or self.ub > _ub:
            self.ub = _ub

    def check(self, sel_val):
        # val = tables[self.table_name].get_sel_by_id(tuple_id)
        return sel_val >= self.lb and sel_val <= self.ub
    
    # we assume that two RangeSelections will only be compared over the same table
    def __str__(self):
        return f"{round(self.lb,2):.2f},{round(self.ub,2):.2f}"
    def __hash__(self):
        return hash(str(self))
    def __eq__(self, other):
        return self.table_name == other.table_name and abs(self.lb - other.lb) < 1e-2 and abs(self.ub - other.ub) < 1e-2


# class Query:
#     def __init__(self, table_names, range_selections=[None, None]):
#         self.table_names = table_names
#         self.range_selections = range_selections
#         self.card = None

#     def set_card(self, card):
#         self.card = card

    # def update_card_by_tuple_update(self, table_name, id, update_type):
    #     assert self.card is not None

    #     cur = 0 if table_name == self.table_names[0] else 1
    #     oth = 1 - cur
    #     oth_table_name = self.table_names[oth]

    #     if self.range_selections[cur] is not None:
    #         if not self.range_selections[cur].check(id):
    #             return

    #     lb, ub = self.range_selections[oth].lb, self.range_selections[oth].ub

    #     jk = tables[table_name].get_jk_by_id(id)
    #     delta = tables[oth_table_name].query(jk, lb, ub)

    #     if update_type == TupleUpdateType.INSERT:
    #         self.card += delta
    #     else:
    #         self.card -= delta

    # def get_bounds_if_pass_local_sel(self, table_name, sel_val):
    #     assert self.card is not None

    #     cur = 0 if table_name == self.table_names[0] else 1
    #     oth = 1 - cur

    #     if self.range_selections[cur] is not None:
    #         if not self.range_selections[cur].check(sel_val):
    #             return None

    #     lb, ub = self.range_selections[oth].lb, self.range_selections[oth].ub
    #     return lb, ub

    # def to_trainingset(self):
    #     return (
    #         self.range_selections[0].lb,
    #         self.range_selections[0].ub,
    #         self.range_selections[1].lb,
    #         self.range_selections[1].ub,
    #         self.card,
    #     )

class QueryInfo:
    # works for twotier_watcher1d for now
    def __init__(self, sql=None, true_card=None, pred=None, plan_cost=None, execution_time=None, execution_time_based_on_model_pred=None, planning_time=None, pc_pred=None, pc_true=None, tableset_to_correction=None):
        self.sql = sql
        self.true_card = true_card
        self.pred = pred
        self.plan_cost = plan_cost
        self.execution_time = execution_time
        self.execution_time_based_on_model_pred = execution_time_based_on_model_pred 
        # note that "execution_time_based_on_model_pred"  
        # can be real (when we really execute the query using 'pred' without any correction)
        # or estimated (when we don't really run it but use some interpolation)
        self.planning_time = planning_time
        self.pc_pred = pc_pred
        self.pc_true = pc_true

        self.tableset_to_correction = None
        self.debug_all_previous_tableset_to_correction = {}
        if tableset_to_correction is not None:
            self.merge_new_join_correction(tableset_to_correction)

    def merge_new_join_correction(self, _new_correction):
        new_correction = copy.deepcopy(_new_correction)
        if self.tableset_to_correction is None:
            self.tableset_to_correction = new_correction
        else:
            for tableset in new_correction:
                if tableset in self.tableset_to_correction:
                    if tableset not in self.debug_all_previous_tableset_to_correction:
                        self.debug_all_previous_tableset_to_correction[tableset] = []
                    self.debug_all_previous_tableset_to_correction[tableset].append(
                        copy.deepcopy(self.tableset_to_correction[tableset])
                    )
                self.tableset_to_correction[tableset] = new_correction[tableset]
                