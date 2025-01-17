package com.swang;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

public class BalancedSlotsTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testBalancedSlots_balance() throws ExecutionException {
        BalancedSlots balancedSlots = new BalancedSlots(10, Duration.ofSeconds(3));
        int numSlots = 3;
        int slotA = balancedSlots.getSlot("A", numSlots);
        int slotB = balancedSlots.getSlot("B", numSlots);
        int slotC = balancedSlots.getSlot("C", numSlots);
        Assert.assertNotEquals(slotA, slotB);
        Assert.assertNotEquals(slotC, slotB);
        System.out.println("testBalancedSlots_balance");
        System.out.println(balancedSlots);
        System.out.println();
    }

    @Test
    public void testBalancedSlots_stable() throws ExecutionException {
        BalancedSlots balancedSlots = new BalancedSlots(10, Duration.ofSeconds(3));
        int numSlots = 3;
        int slotA1 = balancedSlots.getSlot("A", numSlots);
        int slotA2 = balancedSlots.getSlot("A", numSlots);
        Assert.assertEquals(slotA1, slotA2);
        int slotA3 = balancedSlots.getSlot("A", numSlots);
        Assert.assertEquals(slotA2, slotA3);
        System.out.println("testBalancedSlots_stable");
        System.out.println(balancedSlots);
        System.out.println();
    }

    @Test
    public void testBalancedSlots_expire() throws Exception {
        BalancedSlots balancedSlots = new BalancedSlots(10, Duration.ofSeconds(1));
        int numSlots = 3;
        int slotA = balancedSlots.getSlot("A", numSlots);
        Thread.sleep(3000);
        Assert.assertTrue("cache entry is not expired", balancedSlots.getCache().asMap().isEmpty());
        int max = balancedSlots.getSlotMap().values().stream().map(e -> e.keyCount)
                .mapToInt(e -> e).max().getAsInt();
        Assert.assertEquals(0, max);
        System.out.println("testBalancedSlots_expire");
        System.out.println(balancedSlots);
        System.out.println();
    }

    @Test
    public void testBalancedSlots_expand() throws ExecutionException {
        BalancedSlots balancedSlots = new BalancedSlots(10, Duration.ofSeconds(3));
        int numSlots = 3;
        int slotA = balancedSlots.getSlot("A", numSlots);
        int slotB = balancedSlots.getSlot("B", numSlots);
        int slotC = balancedSlots.getSlot("C", numSlots);
        numSlots = 6;
        int slotD = balancedSlots.getSlot("D", numSlots);
        Assert.assertNotEquals(slotA, slotB);
        Assert.assertNotEquals(slotC, slotB);
        Assert.assertNotEquals(slotC, slotD);
        System.out.println("testBalancedSlots_expand");
        System.out.println(balancedSlots);
        System.out.println();
    }

    @Test
    public void testBalancedSlots_shrink() throws ExecutionException {
        BalancedSlots balancedSlots = new BalancedSlots(10, Duration.ofSeconds(3));
        int numSlots = 3;
        int slotA = balancedSlots.getSlot("A", numSlots);
        int slotB = balancedSlots.getSlot("B", numSlots);
        int slotC = balancedSlots.getSlot("C", numSlots);
        numSlots = 1;
        int slotD = balancedSlots.getSlot("D", numSlots);
        int slotE = balancedSlots.getSlot("E", numSlots);
        Assert.assertNotEquals(slotA, slotB);
        Assert.assertNotEquals(slotC, slotB);
        Assert.assertEquals(slotE, slotD);
        System.out.println("testBalancedSlots_shrink");
        System.out.println(balancedSlots);
        System.out.println();
    }

    @Test
    public void testBalancedSlots_threadSafe_1() throws ExecutionException, InterruptedException {
        BalancedSlots balancedSlots = new BalancedSlots(10, Duration.ofSeconds(3));
        int numSlots = 3;
        int loop = 10;
        Thread[] threads = new Thread[loop];
        for (int i = 0; i < loop; i++) {
            threads[i] = new Thread(() -> {
                try {
                    balancedSlots.getSlot("A", numSlots);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        for(int i = 0; i < loop; i++){
            threads[i].start();
        }
        for(int i = 0; i < loop; i++){
            threads[i].join();
        }
        Assert.assertEquals(1, balancedSlots.getCache().size());
        System.out.println("testBalancedSlots_threadSafe_1");
        System.out.println(balancedSlots);
        System.out.println();
    }

    @Test
    public void testBalancedSlots_threadSafe_2() throws ExecutionException, InterruptedException {
        BalancedSlots balancedSlots = new BalancedSlots(10, Duration.ofSeconds(3));
        int numSlots = 3;
        int loop = 10;
        Thread[] threads = new Thread[loop];
        for (int i = 0; i < loop; i++) {
            final int j = i;
            threads[i] = new Thread(() -> {
                try {
                    if(j % 2 == 0) {
                        balancedSlots.getSlot("A", numSlots);
                    }else{
                        balancedSlots.getSlot("B", numSlots);
                    }
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        for(int i = 0; i < loop; i++){
            threads[i].start();
        }
        for(int i = 0; i < loop; i++){
            threads[i].join();
        }
        Assert.assertEquals(2, balancedSlots.getCache().size());
        System.out.println("testBalancedSlots_threadSafe_2");
        System.out.println(balancedSlots);
        System.out.println();
    }

    @Test
    public void testBalancedSlots_balance_2() throws ExecutionException {
        BalancedSlots balancedSlots = new BalancedSlots(120, Duration.ofSeconds(3));
        int numSlots = 3;
        for(int i = 0; i < 100; i++){
            balancedSlots.getSlot("A"+i, numSlots);
        }
        Assert.assertEquals(100, balancedSlots.getCache().size());
        int max = balancedSlots.getSlotMap().values().stream().map(e -> e.keyCount)
                .mapToInt(e -> e).max().getAsInt();
        int min = balancedSlots.getSlotMap().values().stream().map(e -> e.keyCount)
                .mapToInt(e -> e).min().getAsInt();
        Assert.assertEquals(max, min+1);
        Assert.assertEquals(34,max);
        System.out.println("testBalancedSlots_balance_2");
        System.out.println(balancedSlots);
        System.out.println();
    }

}