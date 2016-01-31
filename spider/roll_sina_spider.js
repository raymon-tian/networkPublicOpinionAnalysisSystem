//判断是否有下一页，有则进行渲染
function hasNextPage(){
  var a = document.querySelectorAll('#d_list .pagebox .pagebox_pre a')[1];
  if(a!=null){
    a.click();
    return true;
  }else{
    return false;
  }
}

//判断第一页是否有下一页，有则进行渲染
function hasNextPageForFirstPage(){
  var a = document.querySelector('#d_list .pagebox .pagebox_pre a');
  if(a!=null){
    a.click();
    return true;
  }else{
    return false;
  }
}

//获取单个页面的所有news
function getAllNewsForSinglePage(){
	
	var items = new Array();
	//种类
	var categories = document.querySelectorAll('#d_list ul li .c_chl a');
	//标题
	var titles = document.querySelectorAll('#d_list ul li .c_tit a');
	//时间
	var times = document.querySelectorAll('#d_list ul li .c_time');
	//url
	var urls = document.querySelectorAll('#d_list ul li .c_tit a');
	var sum = categories.length;
	
    for(var i=0;i<sum;++i){
        items[i] = '';
        items[i] += categories[i].innerText+","+titles[i].innerText+","+urls[i].getAttribute('href')+",2016-"+times[i].innerText; 
    }
    return items;
}
//显示单个页面的所有news
function showForSinglePage(items){

    for(var item in items){
      console.log(items[item]+'\n');
    }
}

//将新闻保存本地，采用csv文件
function writeToFile(items){
	if(items == null){
		console.log("第"+count+"页数据未抓取成功"+"\n");
	}else{
		for(var i in items){
			fs.write(path,items[i]+"\n","a");
		}
	}
}

//递归获取数据
function getData(flag){
  if(flag == false){
    phantom.exit();
    return;
  }
  //获取数据，更新状态
  window.setTimeout(function(){
    var itemsNext = new Array();
    var flagNext;
    itemsNext = page.evaluate(getAllNewsForSinglePage);
    ++count;
    showForSinglePage(itemsNext);
    writeToFile(itemsNext);
    //page.render("n"+count+".png");
    flagNext = page.evaluate(hasNextPage);
    console.log("-------------------------------------"+count+"\n");
    console.log("-------------------------------------"+flagNext+"\n");
    getData(flagNext);
  },3000);
}
//处理不同的状态
function handleStatus(status){
  console.log("Status: "+status);
  if(status!=="success"){
    console.log("Fail to load the address");
    phantom.exit();
  }else{
    console.log("Load the address successfully");
    var items;
    items = page.evaluate(getAllNewsForSinglePage);
    showForSinglePage(items);
    writeToFile(items);
    //page.render("n"+count+".png");
    flag = page.evaluate(hasNextPageForFirstPage);
    console.log("-------------------------------------"+count+"\n");
    console.log("-------------------------------------"+flag+"\n");
    getData(flag);
  }
}

phantom.outputEncoding = 'gb2312';

var page = require('webpage').create();
var fs = require('fs');
var path = 'data\\roll_news_sina_com_cn.csv';
//是否有下一页
var flag;
//当前页数
var count = 1;

page.onConsoleMessage = function(msg) {
  console.log(msg);
};
page.open('http://roll.news.sina.com.cn/',handleStatus);

