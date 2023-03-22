package org.test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author 90930
 * @version 1.0
 * @description TODO
 * @date 2023/3/22 0:05
 */
public class CollectionTest {
    static class Person {
        private String name;
        private String phoneNumber;
        // getters and setters

        public String getName() {
            return name;
        }

        public String getPhoneNumber() {
            return phoneNumber;
        }

        public Person(String name, String phoneNumber) {
            this.name = name;
            this.phoneNumber = phoneNumber;
        }
    }

    public static void main(String[] args) {

        List<Person> bookList = new ArrayList<>();
        bookList.add(new Person("jack","18163138123"));
        bookList.add(new Person("martin",null));
        // 空指针异常
        bookList.stream().collect(Collectors.toMap(Person::getName, Person::getPhoneNumber));
    }

}
