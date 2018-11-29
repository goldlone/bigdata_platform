$(function () {
  var id = 0;
  var alertModel = $("#my-alert");
  var modalPreview = $("#modal-preview-data");
  var modalAdd = $("#modal-add-data");
  var modalEdit = $("#modal-edit-data");
  var msgSpan = $("#msg");
  var addFileControl = $("#add-file-control")[0];
  var addDatabaseControl = $("#add-database-control")[0];
  var addHiveColumnControl = $("#add-hive-column-control")[0];
  addFileControl.hidden = true;
  addDatabaseControl.hidden = true;
  addHiveColumnControl.hidden = true;
  var editFileControl = $("#edit-file-control")[0];
  var editDatabaseControl = $("#edit-database-control")[0];
  var editType = "";

  // 监听新增数据源按钮
  $("#add-btn").click(function() {
    modalAdd.modal({
      closeViaDimmer: false,
      width: 800
    });
  });

  // 操作
  $("#table-data").on("click", "a[approach]", function () {
    var approach = $(this)[0].getAttribute("approach");
    id = $(this)[0].getAttribute("data-id");
    console.log(approach + " => " + id);

    switch(approach) {
      case "preview":
        previewData(id);
        break;
      case "edit":
        editType = $(this)[0].getAttribute("data-type");
        editData(id);
        break;
      case "delete":
        $.ajax({
          url: "/dataSource/delete",
          method: "post",
          data: {
            sourceId: id
          },
          success: function (res) {
            showAlert("提交删除成功");
            $("tr[data-id=" + id + "]")[0].remove();
          }
        });
        break;
    }

  });

  // 预览数据
  function previewData(id) {
    $.ajax({
      url: "/dataSource/preview",
      method: "post",
      data: {
        sourceId: id,
        n: 10
      },
      success: function(res) {
        if(res.code === 1001) {
          console.log(res.data);
          var tabHtml = "";
          for(var i=0; i<res.data.length; i++) {
            tabHtml += "<tr><td>"+res.data[i]+"</td></tr>";
          }
          $("#preview-table-body").html(tabHtml);
          modalPreview.modal({
            closeViaDimmer: false,
            width: 800
          });
        } else{
          showAlert(res.msg);
        }
      }
    });
  }

  // 开始 - 新建数据源
  var sourceType = "";
  var mode = "";
  $("input[name='sourceType']").change(function () {
    sourceType = $(this)[0].value;
    changeForm();
  });
  $("input[name='mode']").change(function () {
    mode = $(this)[0].value;
    changeForm();
  });
  function changeForm() {
    if(mode === "file") {
      // show file control
      addFileControl.hidden = false;
      addDatabaseControl.hidden = true;

      editDatabaseControl.hidden = true;
      editFileControl.hidden = false;

      if(sourceType === "hive") {
        // show hive columns
        addHiveColumnControl.hidden = false;
      } else {
        addHiveColumnControl.hidden = true;
      }
    } else if(mode === "mysql") {
      // show mysql control
      addFileControl.hidden = true;
      addHiveColumnControl.hidden = true;
      addDatabaseControl.hidden = false;


      editDatabaseControl.hidden = false;
      editFileControl.hidden = true;
    }
  }
  $("#hive-column").on("click", ".plus", function () {
    $("#other-hive-columns").append($("#hive-column-copy").html());
  });
  $("#hive-column").on("click", ".reduce", function () {
    $(this).parent().parent().remove();
  });
  $("#submit-btn-add").click(function () {

    var fieldData = $("#form").serialize();

    var url = "";
    if(mode === "file") {
      if(sourceType === "hdfs") {
        url = "/file/upload/hdfs";
      } else {
        url = "/file/upload/hive";
      }
      var file = $("input[type='file']")[0].files;
      if(file.length == 0) {
        alert("未选择上传文件");
        return;
      }
      var fileFormData = new FormData();
      fileFormData.append("file", file[0]);
      $.ajax({
        url: url + "?" + fieldData,
        method: "post",
        contentType: false,
        processData: false,
        data: fileFormData,
        success: function (res) {
          if(res.code === 1001) {
            showAlert("提交成功");
          } else {
            showAlert(res.msg);
          }
        }
      });
    } else if(mode === "mysql") {
      if(sourceType === "hdfs") {
        url = "/mysql/import/hdfs";
      } else {
        url = "/mysql/import/hive";
      }
      $.ajax({
        url: url,
        method: "post",
        data: fieldData,
        success: function (res) {
          if(res.code === 1001) {
            showAlert("提交成功");
            $("#modal-add-data").modal('close');
          } else {
            showAlert(res.msg);
          }
        }
      });
    }
  });
  // 结束 - 新建数据源


  // 开始 - 编辑数据源
  function editData(id) {
    modalEdit.modal({
      closeViaDimmer: false,
      width: 800
    });
    $("input[name='sourceId']")[0].value = id;
  }
  $("#submit-btn-edit").click(function () {
    var fieldData = $("#form-edit").serialize();

    var url = "";
    if(mode === "file") {
      if(editType === "hdfs") {
        url = "/file/upload/hdfsPlus";
      } else {
        url = "/file/upload/hivePlus";
      }
      var file = $("input[type='file']")[1].files;
      if(file.length == 0) {
        alert("未选择上传文件");
        return;
      }
      var fileFormData = new FormData();
      fileFormData.append("file", file[0]);
      $.ajax({
        url: url + "?" + fieldData,
        method: "post",
        contentType: false,
        processData: false,
        data: fileFormData,
        success: function (res) {
          if(res.code === 1001) {
            showAlert("提交成功");
          } else {
            showAlert(res.msg);
          }
        }
      });
    } else if(mode === "mysql") {
      if (editType === "hdfs") {
        url = "/mysql/import/hdfsPlus";
      } else {
        url = "/mysql/import/hivePlus";
      }
      $.ajax({
        url: url,
        method: "post",
        data: fieldData,
        success: function (res) {
          if (res.code === 1001) {
            showAlert("提交成功");
          } else {
            showAlert(res.msg);
          }
        }
      });
    }
  });
  // 结束 - 编辑数据源

  function showAlert(msg) {
    msgSpan.html(msg);
    alertModel.modal();
  }
});