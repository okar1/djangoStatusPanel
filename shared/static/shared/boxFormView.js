var contr=new boxFormController(updateGroupListCallback,updateBoxRecListCallback,getSelectedGroup,storeProgressTimeStamp);

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
	contr.ask("group",0)

});

function isNumber(n) {
  return !isNaN(parseFloat(n)) && isFinite(n);
}

function getSelectedGroup(){
	return $('#grouplist').val()
}

function getSelectedBox(){
	return $( ".ui-selected:first" ).attr('id')
}

//status "ok" or "error"
function setBoxStatus(selector,status,progress,errorText){

	if (progress==undefined){
		progress=0
	}

	if (errorText==undefined){
		errorText=""
	}


	if (status=="error"){
		selector.css("background-color","#FF7777").attr("title",errorText).children(":first-child").css({"background-color":"red","width":progress+"%"})
	}
	else{
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

//boxes [{"id": , "name":}]
//boxesStatus [{"id": , "progress": , "error": }]
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
	if (boxesStatus!=undefined && boxesStatus.length>0){
		$("li.meter").map(function(){

			for (var findStatus in boxesStatus){
				if (this.id==boxesStatus[findStatus].id){
					var status=boxesStatus[findStatus]

					if ('error' in status){
						statusText='error'
					}
					else{
						statusText='ok'
					}

					setBoxStatus($("[id='"+this.id+"']"),statusText,status.progress,status.error)
					break
				}
			} 

		})
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


function startTimer(){
	nowTimeStamp=parseInt(new Date().getTime()/1000)
	timerValue=nowTimeStamp-progressClientTimeStamp
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