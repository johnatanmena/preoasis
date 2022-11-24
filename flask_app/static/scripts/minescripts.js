let step = 'step1';
var seconds = 60;
var execution_seconds = 15; // cada 15 segundos preguntarle al servidor como va el proceso
var dashboard_status = true;
//var intervalId = window.setInterval(verify_running_status, 5000); // cada 5 segundos verifica si el proceso se encuentra en ejecución o no 

document.onload = function(){
  next();
  storecheck_progress_track('step1');
  nielsenpan_progress_track('step1')
  
}



function next() {
  step1 = document.getElementById('step1');
  step2 = document.getElementById('step2');
  step3 = document.getElementById('step3');
  step4 = document.getElementById('step4');
  if (step === 'step1') {
    step = 'step2';
    step1.classList.remove("is-active");
    step1.classList.add("is-complete");
    step2.classList.add("is-active");

  } else if (step === 'step2') {
    step = 'step3';
    step2.classList.remove("is-active");
    step2.classList.add("is-complete");
    step3.classList.add("is-active");

  } else if (step === 'step3') {
    step = 'step4d';
    step3.classList.remove("is-active");
    step3.classList.add("is-complete");
    step4.classList.add("is-active");

  } else if (step === 'step4d') {
    step = 'complete';
    step4.classList.remove("is-active");
    step4.classList.add("is-complete");

  } else if (step === 'complete') {
    step = 'step1';
    step4.classList.remove("is-complete");
    step3.classList.remove("is-complete");
    step2.classList.remove("is-complete");
    step1.classList.remove("is-complete");
    step1.classList.add("is-active");
  }
}

function apply_validations(mode){  
  $.ajax({
    url: 'http://localhost:8000/load_nls_val',
    //data: {'date':date},
    type: "GET",
    beforeSend: function(){
      show_loading();
      $('.resultinfo').attr('hidden',true)
      window.interval_function = setInterval(get_message_log, 1000);
    },
    dataType: 'JSON',
    headers: {
      'Access-Control-Allow-Origin': '*'
    },
    success: function(res){
      var cod_status = res[0].cod_status;
      var message_response = res[0].message_response;
      var tmp_files = res[0].tmp_files;
      var qty_files = res[0].qty_files;

      $('.resultcontent').hide();
      $('.resultinfo').removeAttr('hidden');
      hide_loading();
      /* probando resultado de */
      if ( cod_status != -1 ){
        
        if( qty_files == 0 ){
          //poner clase en success
          set_result_info_message('divresultexecution', '<strong>'+ message_response +
            ':</strong> No existen diferencias con catálogos', 'alert-success');
          $('.resultcontent').hide();
        }else{
          set_result_info_message('divresultexecution', '<strong>'+ message_response +
            ':</strong> Revisar catálogos con diferencias marcados a continuación', 'alert-warning');
          $('.resultcontent').show()
          htmlresponse = '<ul><li>' + tmp_files.join('</li><li>') + '</li></ul>';
          $('.resultcontent').html(htmlresponse);
        }
        //habilitar boton publicar catálogo para storecheck
        if(mode == "storecheck"){
          $('#btnstorecheckpublishcatalog').removeAttr('disabled');             
          storecheck_progress_track('step3');
        }else if(mode=="iv_panama"){ // habilitar boton de publicar catálogo 
          $('#btnnielsenpanpublishcatalog').removeAttr('disabled');
          nielsenpan_progress_track('step3');
        }else{
          $('#btnpublishcatalog').removeAttr('disabled');
          next();
        }
      }else{
        // process error case
        $('.resultexecution').html('<strong>Texto de resultado: </strong>'+ "Catalogos publicados con éxito")
        set_result_info_message('divresultexecution', '<strong>Error publicacion catálogo</strong>', 'alert-error');
        $('.resultcontent').hide();
      }
    }, 
    error: function(request, status, error) {
      hide_loading();
      // server error case
      set_result_info_message('divresultexecution', '<strong>Texto de resultado: </strong>'+ request.responseText, 'alert-error');
      $('.resultcontent').hide();
    },
    complete: function(response_object){
      clearInterval(interval_function);
      get_console_log();
      hide_loading();
      $(".seconds").text("Mostrar Log - Ejecución completada");
    }
  });
}

function publish_catalogs(mode){
  $.ajax({
    url: 'http://localhost:8000/load_nls_cat/ans/S',
    //data: {'date':date},
    type: "GET",
    beforeSend: function(){
      show_loading();
      $('.resultinfo').attr('hidden',true)
      window.interval_function = setInterval(get_message_log, 1000);
      $('#confirmcatalogModal').modal('toggle'); // close modal before
    },
    dataType: 'JSON',
    headers: {
      'Access-Control-Allow-Origin': '*'
    },
    success: function(res){
      var cod_status = res.cod_status
      var message_response = res.message_response
      if ( cod_status == 0 ){
        htmlresponse = '<strong>Mensaje de finalización: </strong>'+ message_response
        set_result_info_message('divresultexecution', htmlresponse, 'alert-success');
        //activar siguiente boton
        if(mode == "storecheck"){
          $('#btnstorecheckpublishfiles').removeAttr('disabled');
          storecheck_progress_track('step4');
        }else if(mode == "iv_panama"){
          nielsenpan_progress_track('step4');
          $('#btnnielsenpanpublishfiles').removeAttr('disabled');
        }else{
          $('#btnpublishfiles').removeAttr('disabled');
          next();
        }
      }else{
        htmlresponse = '<strong>Mensaje de finalización: </strong>'+ message_response;
        set_result_info_message('divresultexecution', htmlresponse, 'alert-danger');
      }
    },
    error: function(request, status, error) {
      // server error case
      hide_loading();
      htmlresponse = '<strong>Mensaje de finalización: </strong>'+ request.responseText
      set_result_info_message('divresultexecution', htmlresponse, 'alert-danger');
    },
    complete: function(response_object){
      clearInterval(interval_function);
      get_console_log();
      hide_loading();
      $(".seconds").text("Mostrar Log - Ejecución completada");
    }

  });
}

function publish_files(mode){
  $.ajax({
    url: 'http://localhost:8000/load_nls_ftp/ans/S',
    //data: {'date':date},
    type: "GET",
    beforeSend: function(){
      show_loading();
      $('.resultinfo').attr('hidden',true)
      $('#confirmftpModal').modal('toggle'); //close modal before
      window.interval_function = setInterval(get_message_log, 1000);
      $('.resultcontent').hide();
    },
    dataType: 'JSON',
    headers: {
      'Access-Control-Allow-Origin': '*'
    },
    success: function(res){
      var cod_status = res.cod_status
      var message_response = res.message_response
      var ini_files = res.ini_files
      var qty_published = res.qty_published
      if (cod_status == 0){
        htmlresponse = 'Los archivos: <br> <ul><li>' + ini_files.join('</li><li>') + '</li></ul>Fue(ron) publicado(s) en el servidor FTP';
        $('.resultcontent').html(htmlresponse)
        message_response = '<strong>'+ message_response +': </strong>Se publicaron '+ qty_published + ' en el servidor'
        set_result_info_message('divresultexecution', message_response, 'alert-success');
        if (mode=="storecheck"){
          storecheck_progress_track('step5');
          storecheck_progress_track('step6');
        }else if (mode == "iv_panama"){  
          nielsenpan_progress_track('step5');
          nielsenpan_progress_track('step6');
        }else{
          next();
          next();
        }
      }else{
        // error case
        set_result_info_message('divresultexecution', '<strong>Texto de resultado: </strong>'+ message_response, 'alert-danger');
      }
    },
    error: function(request, status, error) {
      hide_loading();
      // server error case
      set_result_info_message('divresultexecution', '<strong>Mensaje de finalización: </strong>'+ request.responseText, 'alert-danger');
    },
    complete: function(response_object){
      clearInterval(interval_function);
      seconds = 60;
      get_console_log();
      hide_loading();
      $(".seconds").text("Mostrar Log - Ejecución completada");
    }

  });
}

function refresh_catalog(){
  infile = $('#select-catalog-file').val();
  old_val = $('#oldvaluelabel').val();
  new_val = $('#newvaluelabel').val();
    //¿do i have to validate this?
  $.ajax({
    url: encodeURI('http://localhost:8000/update_catalog/infile/'+infile+'/old_val/'+old_val+'/new_val/'+new_val),
    //data: {'date':date},
    type: "GET",
    beforeSend: function(){
      show_loading();
      $('.resultinfo').attr('hidden',true)
      window.interval_function = setInterval(get_message_log, 1000);
      $('.resultcontent').hide();
    },
    dataType: 'JSON',
    headers: {
      'Access-Control-Allow-Origin': '*'
    },
    success: function(res){
      var cod_status = res.cod_status
      var message_response = res.message_response
      if (cod_status == 0){
        set_result_info_message('divresultexecution', '<strong>Ejecución Finalizada: </strong>' + message_response, 'alert-success');
      }else{
        // error case
        set_result_info_message('divresultexecution', '<strong>Texto de resultado: </strong>'+ message_response, 'alert-warning');
      }
    },
    error: function(request, status, error) {
      hide_loading();
      // server error case
      set_result_info_message('divresultexecution', '<strong>Mensaje de finalización: </strong>'+ request.responseText, 'alert-danger');
    },
    complete: function(response_object){
      clearInterval(interval_function);
      seconds = 60;
      get_console_log();
      hide_loading();
      $(".seconds").text("Mostrar Log - Ejecución completada");
    }
  });
}

function nut_refresh_catalog(){
  infile = $('#select-refresh-catalog-file').val();
  old_val = $('#oldrefvaluelabel').val();
  new_val = $('#newrefvaluelabel').val();
  col_val = $('input:radio[name=changevalueRadio]:checked').val();

  $.ajax({
    url: encodeURI('http://localhost:8000/update_catalog/infile/'+infile+'/old_val/'+old_val+'/new_val/'+new_val+ '/column/'+ col_val),
    //data: {'date':date},
    type: "GET",
    beforeSend: function(){
      show_loading();
      $('.resultinfo').attr('hidden',true)
      window.interval_function = setInterval(get_message_log, 1000);
      $('.resultcontent').hide();
    },
    dataType: 'JSON',
    headers: {
      'Access-Control-Allow-Origin': '*'
    },
    success: function(res){
      var cod_status = res.cod_status
      var message_response = res.message_response
      if (cod_status == 0){
        //poner clase en success
        message_response = '<strong>Ejecución Finalizada: </strong>' + message_response 
        set_result_info_message('divresultexecution', message_response, 'alert-success');
      }else{
        // error case
        message_response = '<strong>Ejecución Finalizada: </strong>' + message_response 
        set_result_info_message('divresultexecution', message_response, 'alert-warning');
      }
    },
    error: function(request, status, error) {
      // server error case
      hide_loading();
      message_response = '<strong>Mensaje de finalización: </strong>'+ request.responseText
      set_result_info_message('divresultexecution', message_response, 'alert-danger');
    },
    complete: function(response_object){
      clearInterval(interval_function);
      seconds = 60;
      get_console_log();
      hide_loading();
      $(".seconds").text("Mostrar Log - Ejecución completada");
    }
  });
}


function create_datacheck(){
  new_file = $('#newfilelabel').val().split('\\').pop();
  old_file = $('#oldfilelabel').val().split('\\').pop();
  if (new_file.split('.').pop() != 'csv'){
    $('form').notify('Extensión del archivo nuevo no válida', 'error');
    return false;
  }else if (old_file.split('.').pop() != 'csv' ){
    $('#form').notify('Extensión del archivo anterior no válida', 'error');
    return false;
  }
  $.ajax({
    url: encodeURI('http://localhost:8000/create_datacheck/old_file/' + old_file+'/new_file/'+ new_file),
    //data: {'date':date},
    type: "GET",
    beforeSend: function(){
      show_loading();
      $('.resultinfo').attr('hidden',true)
      window.interval_function = setInterval(get_message_log, 1000);
      $('.resultcontent').hide();
    },
    dataType: 'JSON',
    headers: {
      'Access-Control-Allow-Origin': '*'
    },
    success: function(res){
      var cod_status = res.cod_status
      var message_response = res.message_response
      if (cod_status == 0){
        message_response = '<strong>Ejecución Finalizada: </strong>' + message_response
        set_result_info_message('divresultexecution', message_response, 'alert-success');
      }else{
        message_response = '<strong>Ejecución Finalizada: </strong>' + message_response
        set_result_info_message('divresultexecution', message_response, 'alert-danger');
      }
    },
    error: function(request, status, error) {
      hide_loading();
      // server error case
      $('.divresultexecution').addClass('alert-danger')
      $('.resultexecution').html('<strong>Mensaje de finalización: </strong>'+ request.responseText)
      message_response = '<strong>Ejecución Finalizada: </strong>' + message_response
      set_result_info_message('divresultexecution', message_response, 'alert-danger');
    },
    complete: function(response_object){
      clearInterval(interval_function);
      seconds = 60;
      get_console_log();
      hide_loading();
      $(".seconds").text("Mostrar Log - Ejecución ");
    }
  });

}

function load_files_to_db(){

  $.ajax({
    url: 'http://localhost:8000/load_nut_to_db',
    //data: {'date':date},
    type: "GET",
    beforeSend: function(){
      show_loading();
      $('.myprogressnutresa').attr('hidden',true)
      $('.divresultexecution').hide();
      window.interval_function = setInterval(get_message_log, 1000);
      window.executionstatus_function = setInterval(get_execution_status, 1000);
    },
    dataType: 'JSON',
    headers: {
      'Access-Control-Allow-Origin': '*'
    },
    success: function(res){
      var cod_status = res.cod_status
      var message_response = res.message_response
      if (cod_status == 0){
        //poner clase en success
        message_response = '<strong>Ejecución Finalizada: </strong>' + message_response
        set_result_info_message('divresultexecution', message_response, 'alert-success');
      }
    },
    error: function(request, status, error) {
      // server error case
      hide_loading();
      message_response = '<strong>Mensaje de finalización: </strong>'+ request.responseText
      set_result_info_message('divresultexecution', message_response, 'alert-success');
    },
    complete: function(response_object){
      clearInterval(interval_function);
      clearInterval(executionstatus_function);
      seconds = 60;
      execution_seconds = 15;
      get_console_log();
      $(".seconds").text("Mostrar Log - Ejecución completada");
      $('.text-muted').text("Ejecución Finalizada")
    }
  });
}

function resume_load_execution(){
  $.ajax({
    url: 'http://localhost:8000/nut_process_novelty',
    //data: {'date':date},
    type: "GET",
    beforeSend: function(){
      show_loading();
      $('.myprogressnutresa').attr('hidden',true)
      $('.divresultexecution').hide();
      window.interval_function = setInterval(get_message_log, 1000);
      window.executionstatus_function = setInterval(get_execution_status, 1000);
    },
    dataType: 'JSON',
    headers: {
      'Access-Control-Allow-Origin': '*'
    },
    success: function(res){
      var cod_status = res.cod_status
      var message_response = res.message_response
      $('.resultcontent').hide();
      $('.divresultexecution').show();
      $('.resultinfo').removeAttr('hidden');
      if (cod_status == 0){
        //poner clase en success
        $('.divresultexecution').addClass('alert-success')
        set_result_info_message('divresultexecution', message_response, 'alert-success');
      }else{
        set_result_info_message('divresultexecution', message_response, 'alert-danger');
      }
    },
    error: function(request, status, error) {
      // server error case
      hide_loading();
      set_result_info_message('divresultexecution', '<strong>Mensaje de finalización: </strong>'+ request.responseText, 'alert-danger');
    },
    complete: function(response_object){
      clearInterval(interval_function);
      clearInterval(executionstatus_function);
      seconds = 60;
      execution_seconds = 5; // en el modo de ejecución de resumen se debería  trabajar de manera más rápida
      get_console_log();
      hide_loading();
      $(".seconds").text("Mostrar Log - Ejecución completada");
      $('.text-muted').text("Ejecución Finalizada");
    }
  });
}

function nut_reprocess_load(){
  $.ajax({
    url: 'http://localhost:8000/nut_reprocess_load',
    //data: {'date':date},
    type: "GET",
    beforeSend: function(){
      show_loading();
      $('.myprogressnutresa').attr('hidden',true)
      $('.divresultexecution').hide();
      window.interval_function = setInterval(get_message_log, 1000);
      window.executionstatus_function = setInterval(get_execution_status, 1000);
    },
    dataType: 'JSON',
    headers: {
      'Access-Control-Allow-Origin': '*'
    },
    success: function(res){
      var cod_status = res.cod_status
      var message_response = res.message_response
      $('.resultcontent').hide();
      $('.divresultexecution').show();
      $('.resultinfo').removeAttr('hidden');
      if (cod_status == 0){
        $('.divresultexecution').removeClass('alert-info')
        $('.divresultexecution').removeClass('alert-success')
        $('.divresultexecution').removeClass('alert-danger')
        //poner clase en success
        $('.divresultexecution').addClass('alert-success')
        $('.resultexecution').html('<strong>Ejecución Finalizada: </strong>' + message_response )
      }
    },
    error: function(request, status, error) {
      hide_loading();
      // server error case
      $('.divresultexecution').addClass('alert-danger')
      $('.resultexecution').html('<strong>Mensaje de finalización: </strong>'+ request.responseText)
    },
    complete: function(response_object){
      clearInterval(interval_function);
      clearInterval(executionstatus_function);
      seconds = 60;
      execution_seconds = 15;
      get_console_log();
      hide_loading();
      $(".seconds").text("Mostrar Log - Ejecución completada");
      $('.text-muted').text("Ejecución Finalizada")
    }
  });
}

function delete_catalog_value(){
  infile =  $('#select-delete-catalog-file').val();
  old_val = $('#olddelvaluelabel').val();
  col_val = $('input:radio[name=deletevalueRadio]:checked').val();
  $.ajax({
    url: encodeURI('http://localhost:8000/nut_delete_data_catalog/catalog_name/'+infile+'/old_val/' +old_val+'/exe_mode/'+ col_val),
    //data: {'date':date},
    type: "GET",
    beforeSend: function(){
      show_loading();
      $('.resultinfo').attr('hidden',true)
      window.interval_function = setInterval(get_message_log, 1000);
      $('.resultcontent').hide();
    },
    dataType: 'JSON',
    headers: {
      'Access-Control-Allow-Origin': '*'
    },
    success: function(res){
      var cod_status = res.cod_status
      var message_response = res.message_response
      $('.resultcontent').hide();
      $('.resultinfo').removeAttr('hidden');
      if (cod_status == 0){
        $('.divresultexecution').removeClass('alert-info')
        $('.divresultexecution').removeClass('alert-danger')
        $('.divresultexecution').removeClass('alert-success')
        //poner clase en success
        $('.divresultexecution').addClass('alert-success')
        $('.resultexecution').html('<strong>Ejecución Finalizada: </strong>' + message_response )
        $('.resultcontent').show()
      }else{
        // error case
        $('.divresultexecution').removeClass('alert-info')
        $('.divresultexecution').removeClass('alert-success')
        $('.divresultexecution').removeClass('alert-danger')
        $('.divresultexecution').addClass('alert-success')
        $('.resultexecution').html('<strong>Texto de resultado: </strong>'+ message_response)
        $('.resultcontent').hide()
      }
      $('#confirmdeleteModal').modal('toggle'); // close modal after
    },
    error: function(request, status, error) {
      hide_loading();
      // server error case
      $('.divresultexecution').addClass('alert-danger')
      $('.resultexecution').html('<strong>Mensaje de finalización: </strong>'+ request.responseText)
    },
    complete: function(response_object){
      clearInterval(interval_function);
      seconds = 60;
      get_console_log();
      hide_loading();
      $(".seconds").text("Mostrar Log - Ejecución completada");
    }
  });
}

/*
*función que realiza una petición al servidor de actualizar el log cada X segundos
*/
function get_message_log(){
  seconds =  seconds - 1;
  if (seconds == 0){
    get_console_log();
    seconds = 60
  }
  $(".seconds").text("Mostrar Log - Tiempo para actualización del log: "+ seconds);
}

function get_execution_status(){
  execution_seconds = execution_seconds - 1;
  if(execution_seconds ==0){
    set_execution_message();
    execution_seconds=15
  }
  //show remaining seconds on the front
  $('.text-muted').text("Próxima actualización en " + execution_seconds + " segundos.")
}

function publish_to_s3(){
  year_val = $('#yearvaluelabel').val();
  month_val = $('#monthvaluelabel').val();
  my_url = '';
  if (year_val=="" || month_val=="" ){
    my_url = 'http://localhost:8000/publish_views_to_s3';
  }else {
    my_url = 'http://localhost:8000/publish_views_to_s3/anio/'+ year_val+'/mes/' + month_val;
  }
  $.ajax({
    url: my_url,
    //data: {'date':date},
    type: "GET",
    beforeSend: function(){
      show_loading();
      $('.resultinfo').attr('hidden',true)
      window.interval_function = setInterval(get_message_log, 1000);
      $('.resultcontent').hide();
    },
    dataType: 'JSON',
    headers: {
      'Access-Control-Allow-Origin': '*'
    },
    success: function(res){
      var cod_status = res.cod_status
      var message_response = res.message_response
      var view_published = res.view_published // vistas publicadas 
      $('.resultcontent').hide();
      $('.resultinfo').removeAttr('hidden');
      if (cod_status == 0){
        $('.divresultexecution').removeClass('alert-info')
        $('.divresultexecution').removeClass('alert-danger')
        $('.divresultexecution').removeClass('alert-success')
        //poner clase en success
        $('.divresultexecution').addClass('alert-success')
        $('.resultexecution').html('<strong>Ejecución Finalizada: </strong>' + message_response)
        $('.resultcontent').html( '<strong>Se publicaron las vistas: </strong> '+ view_published)
        $('.resultcontent').show()
      }else{
        // error case
        $('.divresultexecution').removeClass('alert-info')
        $('.divresultexecution').removeClass('alert-success')
        $('.divresultexecution').removeClass('alert-danger')
        $('.divresultexecution').addClass('alert-success')
        $('.resultexecution').html('<strong>Texto de resultado: </strong>'+ message_response)
        $('.resultcontent').hide()
      }
    },
    error: function(request, status, error) {
      hide_loading();
      // server error case
      $('.divresultexecution').addClass('alert-danger')
      $('.resultexecution').html('<strong>Mensaje de finalización: </strong>'+ request.responseText)
    },
    complete: function(response_object){
      clearInterval(interval_function);
      seconds = 60;
      get_console_log();
      hide_loading();
      $(".seconds").text("Mostrar Log - Ejecución completada");
    }
  });

}

/*
* función que permite poner en la vista los resultados de ejecución 
* 
*/
function set_execution_message(){
  $.ajax({
    url: 'http://localhost:8000/get_execution_status',
      type: "GET",
      dataType: 'JSON',
      headers: {
        'Access-Control-Allow-Origin': '*'
      },
      beforeSend: function(){
        $('.myprogressnutresa').removeAttr('hidden')
      },
      success: function(res){
        var cod_status = res.cod_status
        if (cod_status == 0){
          var actual_file = res.actual_file;
          var qty_process_files = res.qty_process_files;
          var qty_error_files =  res.qty_error_files;
          var qty_discard_files = res.qty_discard_files;
          var qty_lot_errors = res.qty_lot_errors;
          var lot_size = res.lot_size;
          var qty_pending_files = res.qty_pending_files;
          var qty_archivos_procesados = res.qty_archivos_procesados;
          var qty_rows = res.qty_rows_act_file;
          var actual_chunk = res.actual_chunk;
          var start_file = res.initial_file_hour;
          var start_process = res.start_process_hour;
          var end_file = res.end_file_hour;
          var end_process = res.end_process_hour;

          
          $('.actual_file').text(actual_file);
          $('.qty_process_files').text(qty_process_files);
          $('.qty_error_files').text(qty_error_files);
          $('.qty_discard_files').text(qty_discard_files);
          $('.qty_pending_files').text(qty_pending_files);
          $('.qty_lot_errors').text(qty_lot_errors);
          $('.lot_size').text(lot_size);
          $('.qty_totales').text(qty_process_files);
          $('.qty_archivos_procesados').text(qty_archivos_procesados);
          $('.qty_procesados').text(qty_archivos_procesados);
          $('.qty_rows').text(qty_rows);
          $('.actual_chunk').text(actual_chunk);
          $('.start_process').text(start_process);
          $('.start_file').text(start_file);
          $('.end_process').text(end_process);
          $('.end_file').text(end_file);


          // calcular porcentaje de avance
          total = qty_process_files;
          if(qty_process_files == 0){
            total = 1 
          }
          percentage = (100 * (qty_archivos_procesados/total)) + "%" 
          $(".loadprogressbar").css('width', percentage)

          //cambiar progress bar de acuerdo al status de archivos con warning y con errores
          if(qty_discard_files != 0 || qty_error_files !=0){
            $(".loadprogressbar").removeClass('bg-info');
            $(".loadprogressbar").removeClass('bg-success');
            $(".loadprogressbar").removeClass('bg-error');
            $(".loadprogressbar").addClass('bg-warning');
          }else{
            $(".loadprogressbar").removeClass('bg-warning');
            $(".loadprogressbar").removeClass('bg-success');
            $(".loadprogressbar").removeClass('bg-error');
            $(".loadprogressbar").addClass('bg-info');
          }
          
          if (percentage == '100%'){
            $('.loadprogressbar').notify('Proceso completado generando informes y enviando correo', 'success')
            if(qty_discard_files != 0 || qty_error_files !=0){
              $.notify('Algunos archivos tuvieron bloques que no pudieron ser procesados revisar log de ejecución o el archivo status.csv', 'warning');
            }else{
              $(".loadprogressbar").removeClass('bg-warning');
              $(".loadprogressbar").removeClass('bg-info');
              $(".loadprogressbar").removeClass('bg-error');
              $(".loadprogressbar").addClass('bg-success');
            }
          }

          
        }
        else{
          $(".loadprogressbar").removeClass('bg-warning');
          $(".loadprogressbar").removeClass('bg-info');
          $(".loadprogressbar").removeClass('bg-success');
          $(".loadprogressbar").addClass('bg-error');
          $(".loadprogressbar").notify(res.message_response, 'error');
        }
      }, 
      error: function(request, status, error) {
        $(".loadprogressbar").removeClass('bg-warning');
        $(".loadprogressbar").removeClass('bg-info');
        $(".loadprogressbar").removeClass('bg-success');
        $(".loadprogressbar").addClass('bg-error');
        $.notify(request.responseText, 'error');
      }
  });
}

/*
 * función que permite la carga de un archivo utilizando petición ajax
 * el archivo utilizado para cambio de tags
 * TODO finish this
 */

function replace_tags(){
  var fd = new FormData();
  var file = $('input#file_tag')[0].files; 

  var filename = $('input#file_tag').val();
  var extension = filename.substring(filename.lastIndexOf('.')+1);
  
  if (extension != 'xlsx') {
    $('#message_tag').text("Formato no válido, se debe incluir un formato .xlsx");
    $('#message_tag').css({'border-color':'red'});
    return
  }

  fd.append('file_tag', file[0]);
  $.ajax({
    url: 'http://localhost:8000/change_tag_process',
    type: "POST",
    data: fd,
    dataType: 'JSON',   
    contentType: false,
    processData: false,
    beforeSend: function(){
      show_loading();
      $('.resultinfo').attr('hidden',true);

    },
    success: function(res) {
      hide_loading();                  
      $('.resultcontent').hide();
      $('.resultinfo').removeAttr('hidden');
      var cod_status = res.cod_status
      if (cod_status != 0){
        $('#message_tag').text("Problema de ejecución, revisar logs del proceso: " + res.message_response);
        $('#message_tag').css({'color':'red'});
      }
      else {
        $('#message_tag').text("Ejecución completada: " + res.message_response);
        $('#message_tag').css({'color':'green'});     
      }
    },
    error: function(e) {
       console.error(e);
       $.notify("Hubo un error en la petición del proceso", "error");
       hide_loading();
      }
    });
  return false;
}

/*
 * función que permite eliminar o desactivar tags de la base de datos debido a un reemplazo
 */

function remove_tags(){
  var fd = new FormData();
  var file = $('input#file_remove_tag')[0].files; 

  var filename = $('input#file_remove_tag').val();
  var extension = filename.substring(filename.lastIndexOf('.')+1);
  
  if (extension != 'xlsx') {
    $('#message_remove_tag').text("Formato no válido, se debe incluir un formato .xlsx");
    $('#message_remove_tag').css({'border-color':'red'});
    return
  }

  fd.append('file_tag', file[0]);
  $.ajax({
    url: 'http://localhost:8000/remove_tag_process',
    type: "POST",
    data: fd,
    dataType: 'JSON',
    contentType: false,
    processData: false,
    beforeSend: function(){
      show_loading();
      $('.resultinfo').attr('hidden',true);

    },
    success: function(res) {
      hide_loading();                  
      $('.resultcontent').hide();
      $('.resultinfo').removeAttr('hidden');
      var cod_status = res.cod_status
      if (cod_status != 0){
        $('#message_remove_tag').text("Problema de ejecución, revisar logs del proceso: " + res.message_response);
        $('#message_remove_tag').css({'color':'red'});
      }
      else {
        $('#message_remove_tag').text("Ejecución completada: " + res.message_response);
        $('#message_remove_tag').css({'color':'green'});     
      }
    },
    error: function(e) {
       console.error(e);
       $.notify("Hubo un error en la petición del proceso", "error");
       hide_loading();
      }
    });
  return false;
}



function start_report_process(){
  datefrom = $('#inlineDateFrom').val();
  dateto = $('#inlineDateTo').val();
  if (!moment(datefrom, 'DD-MM-YYYY',true).isValid()){
    $('#inlineDateFrom').notify('Formato de fecha no válido dd-mm-yyyy', 'error');
    return false;
  }else if(!(moment(dateto, 'DD-MM-YYYY',true).isValid())){
    $('#inlineDateTo').notify('Formato de fecha no válido dd-mm-yyyy', 'error');
    return false;
  }
  $.ajax({
    url: 'http://localhost:8000/create_dashboard/datefrom/'+datefrom+'/dateto/'+ dateto,
      type: "GET",
      timeout: 10000, // set timeout 10 seconds
      dataType: 'JSON',
      headers: {
        'Access-Control-Allow-Origin': 'http://localhost:8050'
      },
      beforeSend:function(){
        show_loading()
        turn_off_server();
        $('.small-box-footer').hide();
      },
      success: function(res){
        var cod_status = res.cod_status
        if (cod_status != 0){
          $.notify(res.message_response, 'error')
        }else{
          $.notify('No hay datos que cumplen esa condición', 'warning')
        }
      }, 

      error: function(request, status, error) { //esto es horrible revisar debe haber una forma mas bonita de hacerlo
        /*
        * Tuve que hacerlo de esta manera porque no he conseguido obtener una respuesta de plotly dash que me indique que 
        * el servidor fue cargado con éxito y que ya está listo para navegación
        */
        if(status == 'timeout'){
          $('.small-box-footer').show();
          $.notify('server running', 'success');
        }
      }, 
      complete: function(){
        hide_loading();
      }
  });
}


function turn_off_server(){
  if (dashboard_status == true){
    $.ajax({
      url: "http://127.0.0.1:8050/shutdown",
      type : "GET",
      beforeSend:function(){
        $('.small-box-footer').hide();
      },
      success: function(res){
        dashboard_status = true;
      },

      error: function(request, status, error) { //esto es horrible revisar debe haber una forma mas bonita de hacerlo
        dashboard_status = false;
      }

    })
  }
  
}

/*
* función para verificar el estado de ejecución e impedir una nueva ejecución si el proceso se encuentra activo
* versión: 2.0 Se agrega lógica para evitar un error de ejecución cuando se llama desde otras vistas o páginas web
* 
*/

function verify_running_status(){
  $.ajax({
    url: 'http://localhost:8000/get_running_status',
    type: "GET",
    success: function(res){
      if (res.cod_status == 0 && res.execution_status != 0) {
        if (!$('.resultcontent').length){
          $('.resultcontent').hide();
        }
        if (!$('.divresultexecution').length ){
          $('.divresultexecution').show();
        }
        if(!$('.resultinfo').length){
          $('.resultinfo').removeAttr('hidden');
        }
        if(!$('.processbtn').length){
          $('.processbtn').prop('disabled', true)
        }
        show_loading();
        set_execution_message()
      }else if(res.cod_status == 0 && res.execution_status == 0){
        if(!$('.processbtn').length){
          $('.processbtn').prop('disabled', false)
        }
        hide_loading();
      }
    },
    error: function(e) {
      console.error(e);
      $.notify("Hubo un error en la petición del proceso", "error");
      hide_loading();
    }

  })
}




/*
* función que se encarga de realizar una petición al servidor para obtener
* el contenido de texto del log
*/
function get_console_log(){
  $.ajax({
    url: 'http://localhost:8000/get_execution_log',
      type: "GET",
      dataType: 'JSON',
      headers: {
        'Access-Control-Allow-Origin': '*'
      },
      success: function(res){                 
        $(".logcontent").text(res)
      }, 
      error: function(request, status, error) {
        $(".logcontent").text(request.responseText)
      }
  });  
}
/*Bootstrap workaround to implement the change on the input file on select eventt*/
$(document).on('change', '.custom-file-input', function() { 
    let fileName = $(this).val().split('\\').pop(); 
    $(this).siblings('.custom-file-label').addClass("selected").html(fileName); 
});





