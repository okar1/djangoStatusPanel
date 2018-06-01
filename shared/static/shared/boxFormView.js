var contr=new boxFormController(updateGroupListCallback,updateBoxRecListCallback,getSelectedGroup,storeProgressTimeStamp);
var updateResizeableHeight=true
var autoMinimizeBoxes=true
var resizeableMinimizedHeight=undefined

// on page load
$( function() {

	function csrfSafeMethod(method) {
	    // these HTTP methods do not require CSRF protection
	    return (/^(GET|HEAD|OPTIONS|TRACE)$/.test(method));
	}
	$.ajaxSetup({
	    beforeSend: function(xhr, settings) {
	        if (!csrfSafeMethod(settings.type) && !this.crossDomain) {
	            xhr.setRequestHeader("X-CSRFToken", token);
	        }
	    }
	});	

	$( "#selectable" ).selectable({stop: updateBoxRecList});
	$('#grouplist').on('change', updateBoxRecList)
	$( "button[id^='button']" ).on("click", btClick );
	$("a.btnMinimize").on("click", btClickMinimize )
	$("a.btnMaximize").on("click", btClickMaximize )
	contr.ask("group",0)

});

(function() {jQuery.fn['bounds'] = function () {
	var bounds = {  
		left: Number.POSITIVE_INFINITY, 
		top: Number.POSITIVE_INFINITY,
		right: Number.NEGATIVE_INFINITY, 
		bottom: Number.NEGATIVE_INFINITY,
		width: Number.NaN,
		height: Number.NaN
	};
	
	this.each(function (i,el) {
		var elQ = $(el);
		var off = elQ.offset();
		off.right = off.left + $(elQ).width();
		off.bottom = off.top + $(elQ).height();
		
		if (off.left < bounds.left)
		bounds.left = off.left;
		
		if (off.top < bounds.top)
		bounds.top = off.top;
		
		if (off.right > bounds.right)
		bounds.right = off.right;
		
		if (off.bottom > bounds.bottom)
		bounds.bottom = off.bottom;
	});
	
	bounds.width = bounds.right - bounds.left;
	bounds.height = bounds.bottom - bounds.top;
	return bounds;
}})();

function isNumber(n) {
  return !isNaN(parseFloat(n)) && isFinite(n);
}

function getSelectedGroup(){
	return $('#grouplist').val()
}

function getSelectedBox(){
	return $( ".ui-selected:first" ).attr('id')
}

//status "ok" or "error" or "disabled"
function setBoxStatus(selector,status,progress,errorText){

	if (progress==undefined){
		progress=0
	}

	if (errorText==undefined){
		errorText=""
	}


	if (status=="error"){
		selector.css("background-color","#FF7777").attr("title",errorText).children(":first-child").css({"background-color":"red","width":progress+"%"})
	} else if (status=="disabled"){
		selector.css("background-color","#545454").removeAttr("title").children(":first-child").css({"background-color":"","width":progress+"%"})
	}else{
		selector.css("background-color","").removeAttr("title").children(":first-child").css({"background-color":"","width":progress+"%"})
	}
	
}

function insertBox(id,name){
	$("#selectable").append(
		$('<li class="ui-state-default ui-selectee meter"/>')
		.attr('id', id)
		.append($('<span style="width: 0%;"/>').append($('<span class="text"/>').text(name)))
	)
}

//groups [{"id": , "name":}]
function updateGroupListCallback(groups){
	$("#grouplist").empty()
	for (var key in groups){
		var item = document.createElement('option');
		item.text=groups[key].name
		item.value=groups[key].id
		$("#grouplist").append(item)
	}
	updateBoxRecList();
};


function updateBoxRecList(){

	selectedGroup=getSelectedGroup()
	selectedBox=getSelectedBox()
	if (selectedBox != undefined){
		contr.ask("boxrec",{'selectedGroup':selectedGroup,"selectedBox":selectedBox})
	}
	else{
		contr.ask("boxrec",{'selectedGroup':selectedGroup})	
	}
}


function btClick(){
	// start button operation. Mark all boxes as non-error and progress=0
	setBoxStatus($("li.ui-selectee"),'ok')
	selected=getSelectedBox()
	if (selected != undefined){
		contr.ask("btclick",{"pressedButton":this.id,"selectedBox":selected })
	}
	else{
		contr.ask("btclick",{"pressedButton":this.id })
	}
	
}

function btClickMinimize(event){
	event.preventDefault();
	if (resizeableMinimizedHeight!=undefined){
		$("#resizable" ).css('height',resizeableMinimizedHeight);	
	}
	else{
		$("#resizable" ).css('height','auto');	
	}
	$("a.btnMinimize").css('width','0');
	$("a.btnMinimize").css('visibility','hidden');
	$("a.btnMaximize").css('visibility','visible');
	$("a.btnMaximize").css('width','100%');
}


function btClickMaximize(event){
	event.preventDefault(); 
	$("#resizable" ).css('height','auto');
	$("a.btnMaximize").css('width','0');
	$("a.btnMaximize").css('visibility','hidden');
	$("a.btnMinimize").css('visibility','visible');
	$("a.btnMinimize").css('width','100%');
}


//boxes [{"id": , "name":}]
//boxesStatus [{"id": , "progress": , "error": ,"enabled":false}]
// enabled is true by default
// boxRec {'selectedBox': ,'records':}
// records ["id": , "name": , style: "add/rem"] (style can be ommited)
//updateNoInteractive true/false
//notUpdateRecords true/false
function updateBoxRecListCallback(boxes, boxesStatus, boxRec,updateNoInteractive,notUpdateRecords){

	if (updateNoInteractive==true){
		$('body').html('<div class="rem">Выполняется обработка, подождите...</div>"')
		return
	}
	
	selectedBox=getSelectedBox()

	// update boxes structure
	if (boxes!=undefined){

		oldBoxes=$("#selectable").children("li") //get all boxes from html
		if (boxes.length==0 || boxes.length!=oldBoxes.length){
			rebuildBoxesNeed=true
		}
		else{
			rebuildBoxesNeed=false
			oldBoxTexts=$("#selectable").find("span.text") //find boxestext stored in .textContent
			for (var box in boxes){
				if (boxes[box].id!=oldBoxes[box].id || boxes[box].name!=oldBoxes[box].textContent)  { 
					rebuildBoxesNeed=true
					break
				}
			}
		}

		if(rebuildBoxesNeed){
			// console.log("update box structure skipped")
			selectedBoxFounded=false
			$("#selectable").empty()
			for (var box in boxes){
				insertBox(boxes[box].id,boxes[box].name)
				if (selectedBox!=undefined && boxes[box].id==selectedBox){
					selectedBoxFounded=true
				}
			}

			//select first box if nothing founded
			if (selectedBoxFounded==false){
				if  (boxes.length>0){
					selectedBox=boxes[0].id
				}
				else{
					selectedBox=undefined
				}
			}

			//mark early selected box as selected after list rebuild
			if (selectedBox!=undefined){
				$("[id='"+selectedBox+"']").addClass("ui-selected") 
			}	
		}
	}

	// update boxes status
	
	// hiding disabled and ok boxes, leaving only ok boxes
	// calculate actual height of container for error items only 
	itemsToUnhide=$()
	errorItemsCount=0
	if (boxesStatus!=undefined && boxesStatus.length>0){
		$("li.meter").map(function(){

			for (var findStatus in boxesStatus){
				if (this.id==boxesStatus[findStatus].id){
					var status=boxesStatus[findStatus]

					selector=$("[id='"+this.id+"']")
					if ('error' in status){
						statusText='error'
						errorItemsCount+=1
					}
					else if ('enabled' in status && status.enabled=="False"){
						statusText='disabled'
						// hiding disabled and ok boxes, leaving only ok boxes
						if (updateResizeableHeight){
							itemsToUnhide=itemsToUnhide.add(selector)
							selector.hide()						
						}
					}else{
						statusText='ok'
						// hiding disabled and ok boxes, leaving only ok boxes
						if (updateResizeableHeight){
							itemsToUnhide=itemsToUnhide.add(selector)
							selector.hide()
						}
					}
					setBoxStatus(selector,statusText,status.progress,status.error)
					break
				}
			} 

		})
		
		if (updateResizeableHeight){
			$("#resizable").css("height","auto")
			// get actual height of container (height of all error items)
			resizeableMinimizedHeight=$("#resizable")[0].getBoundingClientRect().height
			// set this height as constant (if at least 1 error box present)
			if (errorItemsCount>0){
				$("#resizable").css("height",resizeableMinimizedHeight)
			}
			else{
				resizeableMinimizedHeight=undefined
			}
			// and return disabled and ok items to visible state
			itemsToUnhide.show()

			// reset updateResizeableHeight flag. It will be set to true at next timer loop
			updateResizeableHeight=false
		}
	}

	//update records for selected box
	var recordLength=0
	//nothing selected - nothing to update
	if (selectedBox==undefined){
		$("#recordlist").empty()
		$("#summary").text('(Задач в виджете: 0)')
	}
	//we got some records and they for selected box/
	else if (boxRec!=undefined && selectedBox==boxRec.selectedBox ){
		records=boxRec.records

		$("#recordlist").empty()
		for (var record in records){
			var item = document.createElement('option');
			item.text=records[record].name
			//todo:provide boxes id & records id unique for support this
			// item.id=records[record].id
			if ("style" in records[record]){
				var tmp=records[record].style
				if (tmp=="add"){
					$(item).addClass("add")
				}else if (tmp=="rem"){
					$(item).addClass("rem")
				}else if (tmp=="ign"){
					$(item).addClass("ign")
				}
			}
			$("#recordlist").append(item)
		}
		recordLength=records.length
		$("#summary").text('(Задач в виджете: '+recordLength+')')

	}
	//we got some records, but another box is selected now. Ignore received records
	else if ( notUpdateRecords!=true){
		//make +1 request to load new recordlist
		updateBoxRecList()
	}

};




var progressClientTimeStamp=undefined
var lastTimerValue=0

function startTimer(){
	nowTimeStamp=parseInt(new Date().getTime()/1000)
	timerValue=nowTimeStamp-progressClientTimeStamp
	if (timerValue<lastTimerValue){
		if (autoMinimizeBoxes){
			// update height of resizeable once a timer loop
			updateResizeableHeight=true
		}
	}
	lastTimerValue=timerValue
	timerText="Обновлено "+timerValue+" сек назад"
	// reload page every 60 sec if not received server reply in this period
	if (timerValue>timerPageReloadSec && timerValue%60==0){
		location.reload();
	}
	if (timerValue>timerDangerousTimeSec){
		$("#timer").show().text(timerText).removeClass("standartNotice").addClass("dangerousNotice")
	}
	else{
		$("#timer").show().text(timerText).removeClass("dangerousNotice").addClass("standartNotice")
		
		
	}
	//timer tick - every second
	setTimeout(startTimer,1000)
}


function startAutoUpdate(){
	updateBoxRecList()
	// server request for status update - every 10 seconds
	setTimeout(startAutoUpdate,10000)
}

function storeProgressTimeStamp(progressUpdatedAgoSec){
	needStartTimer=(progressClientTimeStamp==undefined)
	progressClientTimeStamp=parseInt(new Date().getTime()/1000)-progressUpdatedAgoSec
	if (needStartTimer){
		startTimer()
		startAutoUpdate()
	}
}