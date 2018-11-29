package cn.goldlone.bigdata_platform.model;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Created by CN on 2018/08/18/0018 19:32 .
 */
public class Page {

    // 当前页码
    private int pageNum;
    // 每页多少数据
    private int pageSize;
    // 总记录条数
    private int totalRecord;

    // 总页数
    private int totalPage;
    // 从第几条数据开始查
    private int offset;

    // 页码开始
    private int start;
    // 页码结束
    private int end;
    // 方便迭代使用
    private List<Integer> pages;

    // 如果为0则，不可翻页
    // 上一页
    private int previous;
    // 上一页
    private int next;


    public Page(int pageNum, int pageSize, int totalRecord) {
        this.pageNum = pageNum;
        this.pageSize = pageSize;
        this.totalRecord = totalRecord;

        if(totalRecord % pageSize == 0)
            totalPage = totalRecord / pageSize;
        else
            totalPage = totalRecord / pageSize + 1;

        offset = (pageNum - 1) * this.pageSize;
        if(offset < 0)
            offset = 0;

        this.start = pageNum - 2;
        this.end = pageNum + 2;

        if(start < 1) {
            start = 1;
            end = 5;
        }
        if(end > totalPage) {
            end = totalPage;
            start = end - 4;
            if(start < 1)
                start = 1;
        }

        previous = pageNum - 1;
        next = pageNum + 1;
        if(previous < 1)
            previous = 0;
        if(next > totalPage)
            next = 0;
    }

    public int getPageNum() {
        return pageNum;
    }

    public void setPageNum(int pageNum) {
        this.pageNum = pageNum;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getTotalRecord() {
        return totalRecord;
    }

    public void setTotalRecord(int totalRecord) {
        this.totalRecord = totalRecord;
    }

    public int getTotalPage() {
        return totalPage;
    }

    public void setTotalPage(int totalPage) {
        this.totalPage = totalPage;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public int getPrevious() {
        return previous;
    }

    public void setPrevious(int previous) {
        this.previous = previous;
    }

    public int getNext() {
        return next;
    }

    public void setNext(int next) {
        this.next = next;
    }

  public List<Integer> getPages() {
    List<Integer> pages = new ArrayList<>();
    for(int i=start; i<=end;  i++) {
      pages.add(i);
    }
    return pages;
  }

  @Override
    public String toString() {
        return "Page{" +
                "pageNum=" + pageNum +
                ", pageSize=" + pageSize +
                ", totalRecord=" + totalRecord +
                ", totalPage=" + totalPage +
                ", offset=" + offset +
                ", start=" + start +
                ", end=" + end +
                ", previous=" + previous +
                ", next=" + next +
                '}';
    }
}
