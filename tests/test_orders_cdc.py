"""Automated regression tests for RetailCo ingestion and recovery. Tests use isolated fixtures where possible instead of a live SQL Server database."""

from pathlib import Path
import sys
import pandas as pd
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from orders_cdc import apply_changes,active_orders

Z="00000000000000000000"
def event(key, op, n, **values):
    """Support this module's processing or test workflow; inspect callers for the expected input and output contract."""
    return {"OrderID":key,"operation":op,"change_lsn":f"{n:020X}","seqval":Z,
            "CustomerID":values.get("CustomerID",10),"OrderDate":values.get("OrderDate","2026-01-01"),
            "OrderStatus":values.get("OrderStatus","Pending"),"TotalAmount":values.get("TotalAmount",100)}
    """Support this module's processing or test workflow; inspect callers for the expected input and output contract."""
def apply(state,*rows):return apply_changes(state,pd.DataFrame(rows))

def test_insert_update_delete_and_tombstone():
    """Regression test documenting expected behavior for insert update delete and tombstone."""
    s=apply(pd.DataFrame(),event(1,"I",1))
    s=apply(s,event(1,"U",2,OrderStatus="Shipped"))
    assert s.iloc[0].OrderStatus=="Shipped"
    s=apply(s,event(1,"D",3))
    assert bool(s.iloc[0].is_deleted)
    assert active_orders(s).empty

def test_duplicate_delivery_does_not_create_duplicate_order():
    """Regression test documenting expected behavior for duplicate delivery does not create duplicate order."""
    e=event(1,"I",1)
    s=apply(pd.DataFrame(),e,e)
    assert len(s)==1
    assert apply(s,e).equals(s)

def test_reordered_old_update_cannot_resurrect_deleted_row():
    """Regression test documenting expected behavior for reordered old update cannot resurrect deleted row."""
    s=apply(pd.DataFrame(),event(1,"I",1),event(1,"D",5),event(1,"U",3))
    assert bool(s.iloc[0].is_deleted)

def test_multiple_orders_and_out_of_order_events():
    """Regression test documenting expected behavior for multiple orders and out of order events."""
    s=apply(pd.DataFrame(),event(2,"I",2),event(1,"U",3,OrderStatus="Delivered"),event(1,"I",1))
    assert list(s.OrderID)==[1,2]
    assert s.iloc[0].OrderStatus=="Delivered"

def test_conflicting_same_position_rejected():
    """Regression test documenting expected behavior for conflicting same position rejected."""
    with pytest.raises(ValueError,match="Conflicting"):
        apply(pd.DataFrame(),event(1,"I",1,TotalAmount=5),event(1,"I",1,TotalAmount=6))

def test_unknown_operation_rejected():
    """Regression test documenting expected behavior for unknown operation rejected."""
    with pytest.raises(ValueError,match="Unsupported"):
        apply(pd.DataFrame(),event(1,"BEFORE",1))

def test_bad_lsn_rejected():
    """Regression test documenting expected behavior for bad lsn rejected."""
    broken=event(1,"I",1);broken["change_lsn"]="garbage"
    with pytest.raises(ValueError,match="20-character"):
        apply(pd.DataFrame(),broken)

def test_delete_before_insert_tombstone_blocks_stale_insert():
    """Regression test documenting expected behavior for delete before insert tombstone blocks stale insert."""
    s=apply(pd.DataFrame(),event(1,"D",4))
    s=apply(s,event(1,"I",1))
    assert bool(s.iloc[0].is_deleted)
