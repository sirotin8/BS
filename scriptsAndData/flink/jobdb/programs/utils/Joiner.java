package de.hshannover.vis.flink.jobdb.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

// for CoGroup-Functions in Flink (worst API ever met)
@SuppressWarnings("serial")
public class Joiner<Left extends Tuple, Right extends Tuple> 
implements CoGroupFunction<Left,Right,Left> {

	public enum Mode {
		FULL(true, true), LEFT(true, false), RIGHT(false, true),
		LEFTONLY(true,false,false)
//		,RIGHTONLY(false,true,false)
		;
		private boolean left, right,inner;

		private Mode(boolean left, boolean right) {
			this.left = left;
			this.right = right;
			this.inner = true;
		}

		private Mode(boolean left, boolean right,boolean inner) {
			this.left = left;
			this.right = right;
			this.inner = inner;
		}
		
		public boolean doLeft() {
			return this.left;
		}

		public boolean doRight() {
			return this.right;
		}

		public boolean doFull() {
			return this.right && this.left;
		}
		public boolean doInner(){
			return this.inner;
		}
	}
	
	private Left lNull = null;
	private Right rNull = null;
	private Mode mode;
	private Class<Left> leftTuple;
	private Constructor<Left> cons;
	private boolean knowClass = false;
	private int offset = 0;
	
	@SuppressWarnings("unchecked")
	public Joiner(Mode mode,Object myNull){
				
		this.mode = mode;
		switch(mode){
		case LEFTONLY:
		case LEFT:
			this.rNull = (Right)myNull;
			break;
//		case RIGHTONLY:
		case RIGHT:
			this.lNull = (Left)myNull;
			break;
		case FULL:
			throw new IllegalArgumentException("Full-outer-join needs "
					+ "null-objects on both sides");
		}
	}

	public Joiner(Left lNull, Right rNull) {
		super();
		mode = Mode.FULL;
		this.lNull = lNull;
		this.rNull = rNull;
	}
	
	private boolean knowRight = false;
	Constructor<Right> rCons = null;
	
	@SuppressWarnings("unchecked")
	private Right copyTuple(Right tuple){
		
		if(!knowRight){
			int cols = tuple.getArity();
			try {
				Class<Right> cls = (Class<Right>)Class.forName("org.apache.flink.api.java."
						+ "tuple.Tuple" + cols);
				rCons = cls.getConstructor();
			} catch (ClassNotFoundException | NoSuchMethodException |
					SecurityException | IllegalArgumentException e) {
				e.printStackTrace();
			}
			knowRight = true;
		}
		
		Right result = null;
		try {
			result = rCons.newInstance();
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
			e.printStackTrace();
		}
		for(int i=0;i<tuple.getArity();i++){
			result.setField(tuple.getField(i), i);
		}
		return result;
	}

	private void doFull(Iterable<Left> left, Left lNull, Iterable<Right> right,
			Right rNull, Collector<Left> out) {
		Iterator<Left> iLeft = left.iterator();
		Iterator<Right> iRight = right.iterator();

		Left tLeft;
		Right tRight;
		l=0;r=0;

		ArrayList<Right> rList = new ArrayList<Right>();

		if (iLeft.hasNext()) {
			if (iRight.hasNext() && mode.doInner()) {
				// no outer, everyone has its own tuple-partner
				// ( or even more - therefore the loop)
				while (iRight.hasNext()) {
					// first copying all right-objects, because you can only
					// iterate once in flink; the references will change later
					// therefore we ve to copy ;-0
					rList.add(copyTuple(iRight.next()));
				}
				while (iLeft.hasNext()) {
					l++;
					// now n of left multiply by n of right
					tLeft = iLeft.next();
					for (Right tmpRight : rList) {
						r++;
						out.collect(newTuple(tLeft, tmpRight));
					}
				}
			} else if (mode.doLeft()) {
				// left-outer, no tuple on the right side
				while (iLeft.hasNext()) {
					l++;
					tLeft = iLeft.next();
					out.collect(newTuple(tLeft, rNull));
				}
			}
		} else {
			if (iRight.hasNext() && mode.doRight()) {
				while (iRight.hasNext()) {
					// right-outer, no tuple on the left side
					tRight = iRight.next();
					out.collect(newTuple(lNull, tRight));
				}
			} else if (mode.doFull()) {
				// full-outer, no tuple exist
				out.collect(newTuple(lNull, rNull));
			}
		}
	}
	
	private int l,r;
	
	@SuppressWarnings("unchecked")
	private Left newTuple(Left left, Right right){
		Left result = null;
		if(!knowClass){
			int cols = left.getArity();
			this.offset = cols - right.getArity();
			try {
				this.leftTuple = (Class<Left>)Class.forName("org.apache.flink.api.java."
						+ "tuple.Tuple" + cols);
				this.cons = this.leftTuple.getConstructor();
			} catch (ClassNotFoundException | NoSuchMethodException |
					SecurityException | IllegalArgumentException e) {
				e.printStackTrace();
			}
			knowClass = true;
		}

		try {
			result = this.cons.newInstance();
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
			e.printStackTrace();
		}
		int i;
		for(i=0;i<offset;i++){
//			if(i==0){
//				result.setField(l+": "+left.getField(i), i);
//			}else
			result.setField(left.getField(i), i);
		}
		for(int j=0;j<right.getArity();j++){
//			if(j==0){
//				result.setField(r+": "+right.getField(j), i);
//			}else
			result.setField(right.getField(j), i);
			i++;
		}
		return result;
	}

	public void fullLefter(Iterable<Left> left,
			Left lNull, Iterable<Right> right, Right rNull,
			Collector<Left> out) {
		this.doFull(left, lNull, right, rNull, out);
	}

	public void rightLefter(Iterable<Left> left,
			Left lNull, Iterable<Right> right,
			Collector<Left> out) {
		this.doFull(left, lNull, right, null, out);
	}

	public void leftLefter(Iterable<Left> left,
			Iterable<Right> right, Right rNull,
			Collector<Left> out) {
		this.doFull(left, null, right, rNull, out);
	}

	@Override
	public void coGroup(Iterable<Left> left, Iterable<Right> right,
			Collector<Left> out) throws Exception {

		switch(this.mode){
		case FULL:
			this.fullLefter(left, lNull, right, rNull,out);
			break;
//		case RIGHTONLY:
		case RIGHT:
			this.rightLefter(left, lNull, right, out);
			break;
		case LEFTONLY:
		case LEFT:
			this.leftLefter(left, right, rNull, out);
			break;
		}
		
	}

}
