package project;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortBeans implements WritableComparable<SortBeans> {
    public Integer num;

    public SortBeans(){

    }
    public SortBeans(Integer num){

        this.num=num;
    }
    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    @Override
    public int compareTo(SortBeans o) {

        /**
         * 相等的时候，使得不为0，从而就不会对key进行合并
         */
        if(this.num==o.num){
            return 1;
        }else{
           return this.num-o.num;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeInt(this.num);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.num=dataInput.readInt();
    }

    @Override
    public String toString() {
        return "SortBeans{" +
                "num=" + num +
                '}';
    }
}
