package hadoop.stack.popular;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class TextPair implements WritableComparable<TextPair> {

	public static class SortComparator extends WritableComparator {

		private static final Text.Comparator TEXT_COMP = new Text.Comparator();

		public SortComparator() {
			super(TextPair.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int result = 0;
			try {
				int id1L = WritableUtils.decodeVIntSize(b1[s1])
						+ readVInt(b1, s1);
				int id2L = WritableUtils.decodeVIntSize(b2[s2])
						+ readVInt(b2, s2);

				result = TEXT_COMP.compare(b1, s1, id1L, b2, s2, id2L);

				 if (result == 0)
				 result = TEXT_COMP.compare(b1, s1 + id1L, l1 - id1L, b2, s2
				 + id2L, l2 - id2L);

			} catch (IOException e) {
				e.printStackTrace();
			}

			return result;
		}
	}

	public static class GroupComparator extends WritableComparator {

		private static final Text.Comparator TEXT_COMP = new Text.Comparator();

		public GroupComparator() {
			super(TextPair.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int result = 0;
			try {
				int id1L = WritableUtils.decodeVIntSize(b1[s1])
						+ readVInt(b1, s1);
				int id2L = WritableUtils.decodeVIntSize(b2[s2])
						+ readVInt(b2, s2);

				result = TEXT_COMP.compare(b1, s1, id1L, b2, s2, id2L);

			} catch (IOException e) {
				e.printStackTrace();
			}
			return result;
		}

		@SuppressWarnings("all")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			if (a instanceof TextPair && b instanceof TextPair) {
				return ((TextPair) a).first.compareTo(((TextPair) b).first);
			}
			return super.compare(a, b);
		}
	}

	static {
		WritableComparator.define(TextPair.class, new GroupComparator());
	}

	public Text first, second;

	public TextPair() {
		first = new Text();
		second = new Text();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public int compareTo(TextPair other) {
		int result = first.compareTo(other.first);
		 if (result == 0)
		 result = second.compareTo(other.second);
		return result;
	}

	@Override
	public String toString() {
		return first + " | " + second + " : ";
	}

}
