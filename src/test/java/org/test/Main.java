package org.test;

import org.junit.Test;

import java.util.*;

public class Main {
    public static void main(String[] args) {

        (from) -> Integer.valueOf(from);

        Converter<String, Integer> converter = Integer::valueOf;

        List<Integer> numbers = new ArrayList<>();
        numbers.add(5);
        numbers.add(3);
        numbers.add(1);

        // 使用 sort 默认方法按升序排序
        numbers.sort(Comparator.naturalOrder());

        System.out.println(numbers); // 输出：[1, 3, 5]
    }

    @Test
    public void parallelStreamTest() {

        List<Integer> numbers = Arrays.asList(1, 2, 5, 4);
        numbers.parallelStream().forEach(num -> System.out.println(Thread.currentThread().getName() + ">>" + num));
    }

}