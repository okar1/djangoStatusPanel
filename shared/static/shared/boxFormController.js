//исправить на метод класса если потребуется более 1 экземпляра controller
//(хз как)
var thisBfcClass

var boxApplyWorking=false
// not interactive update mode (without displaying progress)
var updateNoInteractive=false

var updateTimePeriod=500

class boxFormController{

	constructor(updateGroupListCallback,updateBoxListCallback,getSelectedGroup,storeProgressTimeStamp){
		this.ugl=updateGroupListCallback;
		this.ubl=updateBoxListCallback;
		this.getSelectedGroup=getSelectedGroup;
		this.storeProgressTimeStamp=storeProgressTimeStamp
		thisBfcClass=this
	};


	_logError(jqXHR ,  textStatus,  errorThrown) {
		boxApplyWorking=false
		console.log(jqXHR);
		console.log(textStatus);
		console.log(errorThrown);
	}

	//we cannot use "this" here because it means ajax request, not a class instance
	//use "thisBfcClass" instead
	_boxApplyStatus(reply, textStatus, jqXHR){
		if ( (!boxApplyWorking) || updateNoInteractive){
			return
		}

		// console.log(reply)
		//reply.progress[0]=1 is a temporally lock while server calculates boxapplystatus
		if (typeof(reply) != "undefined" && "mode" in reply && "progress" in reply  &&
			"boxes" in reply  && reply.mode=="boxrec")  {

			if (reply.progress[0]==1){
				updateNoInteractive=true
			}
			thisBfcClass.ubl(reply.boxes,reply.progress,undefined,updateNoInteractive,true)  
		}

		if (!updateNoInteractive){
			setTimeout(function(){
				$.ajax({
					type: "POST",
					data: JSON.stringify({"mode":"boxrec","data":{'selectedGroup':thisBfcClass.getSelectedGroup()}}),
					contentType: "application/json; charset=utf-8",
					dataType: "json",
					success: thisBfcClass._boxApplyStatus,
					error: thisBfcClass._logError
				});
			},updateTimePeriod)
		}
	}

	//send ajax request to server
	//mode: "group" "boxrec" "btclick"
	//data: {'selectedGroup': ,"selectedBox": , "pressedButton":})
	ask(mode,data){
		var self=this
		$.ajax({
			type: "POST",
			//url: "/cvUpdate/",
			data: JSON.stringify({"mode":mode,"data":data}),
			contentType: "application/json; charset=utf-8",
			dataType: "json",
					
			//reply:  {"mode": ,'message':, 'groups':, 'boxes':, 'boxrec':, 'progress'}
			success: function(reply, textStatus, jqXHR){
				// console.log(reply)
				if ("mode" in reply){

					if (reply.mode=="group" && "groups" in reply){
						self.ugl(reply.groups)
					}
					
					if (reply.mode=="boxrec"  && "boxes" in reply && "progress" in reply ){
						self.ubl(reply.boxes,reply.progress,reply.boxrec)	
					}
					
					if (mode=="btclick"){
						// end button operation
						self.ubl(undefined,reply.progress,reply.boxrec)	
						boxApplyWorking=false
						
						if (updateNoInteractive){
							location.reload();
							return
						}
					}
					
					if ("progressupdatedagosec" in reply){
						self.storeProgressTimeStamp(reply.progressupdatedagosec)
					}

					if ("message" in reply && reply.message!=""){
						setTimeout(
							function(){alert(reply.message)}
						,updateTimePeriod)}

				}
			},
			error: thisBfcClass._logError
		});

		//start button operation. Start operation status check.
		if (mode=="btclick"){
			boxApplyWorking=true
			setTimeout(this._boxApplyStatus,updateTimePeriod)
		}
	}

};