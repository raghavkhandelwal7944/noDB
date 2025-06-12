package com.example.dataprocessor.model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.LocalDate;

@Entity
@Table(name = "sales_data")
public class SalesData {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String segment;
    private String country;
    private String product;
    private String discountBand;
    private BigDecimal unitsSold;
    private BigDecimal manufacturingPrice;
    private BigDecimal salePrice;
    private BigDecimal grossSales;
    private BigDecimal discounts;
    private BigDecimal sales;
    private BigDecimal cogs;
    private BigDecimal profit;
    private LocalDate date;
    private Integer monthNumber;
    private String monthName;
    private Integer year;

    // Getters and Setters
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSegment() {
        return segment;
    }

    public void setSegment(String segment) {
        this.segment = segment;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public String getDiscountBand() {
        return discountBand;
    }

    public void setDiscountBand(String discountBand) {
        this.discountBand = discountBand;
    }

    public BigDecimal getUnitsSold() {
        return unitsSold;
    }

    public void setUnitsSold(BigDecimal unitsSold) {
        this.unitsSold = unitsSold;
    }

    public BigDecimal getManufacturingPrice() {
        return manufacturingPrice;
    }

    public void setManufacturingPrice(BigDecimal manufacturingPrice) {
        this.manufacturingPrice = manufacturingPrice;
    }

    public BigDecimal getSalePrice() {
        return salePrice;
    }

    public void setSalePrice(BigDecimal salePrice) {
        this.salePrice = salePrice;
    }

    public BigDecimal getGrossSales() {
        return grossSales;
    }

    public void setGrossSales(BigDecimal grossSales) {
        this.grossSales = grossSales;
    }

    public BigDecimal getDiscounts() {
        return discounts;
    }

    public void setDiscounts(BigDecimal discounts) {
        this.discounts = discounts;
    }

    public BigDecimal getSales() {
        return sales;
    }

    public void setSales(BigDecimal sales) {
        this.sales = sales;
    }

    public BigDecimal getCogs() {
        return cogs;
    }

    public void setCogs(BigDecimal cogs) {
        this.cogs = cogs;
    }

    public BigDecimal getProfit() {
        return profit;
    }

    public void setProfit(BigDecimal profit) {
        this.profit = profit;
    }

    public LocalDate getDate() {
        return date;
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }

    public Integer getMonthNumber() {
        return monthNumber;
    }

    public void setMonthNumber(Integer monthNumber) {
        this.monthNumber = monthNumber;
    }

    public String getMonthName() {
        return monthName;
    }

    public void setMonthName(String monthName) {
        this.monthName = monthName;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }
} 