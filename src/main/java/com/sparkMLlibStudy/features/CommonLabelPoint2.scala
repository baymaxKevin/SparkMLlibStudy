package com.sparkMLlibStudy.features

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 18-10-29 上午9:42
  * @Modified By:
  */
class CommonLabelPoint2 extends Serializable {

  /** ============================Common features============================ */
  /**
    * 日志类型
    */
  var logType: String = ""
  /**
    * 处理批次ID
    */
  var batchNo: String = ""
  /**
    * 标签(点击或曝光)
    */
  var label: String = ""
  /**
    * 用户ID
    */
  var userId: String = ""
  /**
    * 帖子或资讯ID
    */
  var itemId: String = ""

  /** ============================GA features============================ */
  /**
    * 请求时间
    */
  var userRequestTime: String = ""
  /**
    * 内容类型，1：帖子，2：资讯
    */
  var itemDataType: String = ""
  /**
    * 手机运营商
    */
  var userOt: String = ""
  /**
    * IP
    */
  var userIP: String = ""
  /**
    * 手机系统
    */
  var userOs: String = ""
  /**
    * 网络类型
    */
  var userApn: String = ""
  /**
    * 栏目页id
    */
  var itemCatId: String = ""
  /**
    * 算法来源类型
    */
  var itemAlgSource: String = ""
  /**
    * 下翻itemId
    */
  var itemFloor: String = ""

  /** ============================User features============================ */
  /**
    * 用户身份 0：经期，1：怀孕，2：备孕，3：辣妈
    */
  var userMode: String = ""
  /**
    * 用户所在的城市编号
    */
  var userCity: String = ""
  /**
    * 用户所在城市级别
    */
  var userCityLevel: String = ""
  /**
    * 用户是否结婚 0：否，1：是
    */
  var userMarr: String = ""
  /**
    * 用户是否怀孕 0：否，1：是
    */
  var userGest: String = ""
  /**
    * 年龄
    */
  var userAge: String = ""
  /**
    * 宝宝年龄
    */
  var userBage: String = ""
  /**
    * app版本
    */
  var userAppV: String = ""
  /**
    * 点击
    */
  var userCliN_Inc = ""
  /**
    * 曝光
    */
  var userShoN_Inc = ""
  /**
    * 收藏
    */
  var userColN: String = ""
  /**
    * 评论
    */
  var userRevi: String = ""
  /**
    * 举报
    */
  var userRepN: String = ""
  /**
    * 分享
    */
  var userShare: String = ""
  /**
    * 评论点赞
    */
  var userLireN = ""
  /**
    * 点击查看评论
    */
  var userVreN: String = ""
  /**
    *
    */
  var userBotActCt: String = ""
  /**
    * 用户主题标签61个
    */
  var userTtP: String = ""
  /**
    * 用户关键词
    */
  var userKtW: String = ""
  /**
    * 阅读时间为白天或晚上
    */
  var userDayOrNight: String = ""
  /**
    * 用户系统跟网络组合
    */
  var userOsApn: String = ""
  /**
    * 用户网络跟运营商组合
    */
  var userApnOt: String = ""
  /**
    * 点击率
    */
  var userCtr: String = ""
  /**
    * 阅读总时长
    */
  var userTotalTime: String = ""
  /**
    * 浏览到底部次数
    */
  var userView2BottomTimes: String = ""
  /**
    * 有效阅读次数
    */
  var userEffTimes: String = ""
  /**
    * 用户年龄和宝宝年龄差
    */
  var userBageAgeMinus: String = ""
  /**
    * 怀孕和结婚组合
    */
  var userGestMarr: String = ""
  /**
    * 系统和运营商组合
    */
  var userOsOt: String = ""
  /**
    * 机型
    */
  var userUa: String = ""
  /**
    * 偏好特征
    */
  var userFloorP: String = ""
  var userNewsTypeP: String = ""
  var userTagPC4: String = ""
  var userTagPC2: String = ""
  var userFloorPD: String = ""
  var userNewsTypePD: String = ""
  var userTagPDC4: String = ""
  var userTagPDC2: String = ""
  /**
    * 用户1天、2天、3天、5天、7天点击数
    */
  var userCliN_Inc1d: String = ""
  var userCliN_Inc2d: String = ""
  var userCliN_Inc3d: String = ""
  var userCliN_Inc5d: String = ""
  var userCliN_Inc7d: String = ""
  /**
    * 用户1天、2天、3天、5天、7天暴光数
    */
  var userShoN_Inc1d: String = ""
  var userShoN_Inc2d: String = ""
  var userShoN_Inc3d: String = ""
  var userShoN_Inc5d: String = ""
  var userShoN_Inc7d: String = ""
  /**
    * 用户1天、2天、3天、5天、7天点击率
    */
  var userCtr1d: String = ""
  var userCtr2d: String = ""
  var userCtr3d: String = ""
  var userCtr5d: String = ""
  var userCtr7d: String = ""
  /**
    * 用户1天、2天、3天、5天、7天有效阅读次数
    */
  var userEffTimes1d: String = ""
  var userEffTimes2d: String = ""
  var userEffTimes3d: String = ""
  var userEffTimes5d: String = ""
  var userEffTimes7d: String = ""
  /**
    * 用户1天、2天、3天、5天、7天下拉到底部次数
    */
  var userView2BottomTimes1d: String = ""
  var userView2BottomTimes2d: String = ""
  var userView2BottomTimes3d: String = ""
  var userView2BottomTimes5d: String = ""
  var userView2BottomTimes7d: String = ""
  /**
    * 用户1天、2天、3天、5天、7天总停留时长
    */
  var userTotalTime1d: String = ""
  var userTotalTime2d: String = ""
  var userTotalTime3d: String = ""
  var userTotalTime5d: String = ""
  var userTotalTime7d: String = ""
  /**
    * 用户1天、2天、3天、5天、7天平均停留时长
    */
  var userAvsT1d: String = ""
  var userAvsT2d: String = ""
  var userAvsT3d: String = ""
  var userAvsT5d: String = ""
  var userAvsT7d: String = ""
  /**
    * 用户1天、2天、3天、5天、7天阅读帖子占比
    */
  var userRead1Rate1d: String = ""
  var userRead1Rate2d: String = ""
  var userRead1Rate3d: String = ""
  var userRead1Rate5d: String = ""
  var userRead1Rate7d: String = ""
  /**
    * 用户1天、2天、3天、5天、7天阅读资讯占比
    */
  var userRead2Rate1d: String = ""
  var userRead2Rate2d: String = ""
  var userRead2Rate3d: String = ""
  var userRead2Rate5d: String = ""
  var userRead2Rate7d: String = ""
  /**
    * 用户1小时、2小时、6小时、12小时点击数
    */
  var userCliN_Inc1h: String = ""
  var userCliN_Inc2h: String = ""
  var userCliN_Inc6h: String = ""
  var userCliN_Inc12h: String = ""
  /**
    * 用户1小时、2小时、6小时、12小时暴光数
    */
  var userShoN_Inc1h: String = ""
  var userShoN_Inc2h: String = ""
  var userShoN_Inc6h: String = ""
  var userShoN_Inc12h: String = ""
  /**
    * 用户1小时、2小时、6小时、12小时点击率
    */
  var userCtr1h: String = ""
  var userCtr2h: String = ""
  var userCtr6h: String = ""
  var userCtr12h: String = ""
  /**
    * 用户1小时、2小时、6小时、12小时有效阅读次数
    */
  var userEffTimes1h: String = ""
  var userEffTimes2h: String = ""
  var userEffTimes6h: String = ""
  var userEffTimes12h: String = ""
  /**
    * 用户1小时、2小时、6小时、12小时下拉到底部次数
    */
  var userView2BottomTimes1h: String = ""
  var userView2BottomTimes2h: String = ""
  var userView2BottomTimes6h: String = ""
  var userView2BottomTimes12h: String = ""
  /**
    * 用户1小时、2小时、6小时、12小时总停留时长
    */
  var userTotalTime1h: String = ""
  var userTotalTime2h: String = ""
  var userTotalTime6h: String = ""
  var userTotalTime12h: String = ""
  /**
    * 用户1小时、2小时、6小时、12小时平均停留时长
    */
  var userAvsT1h: String = ""
  var userAvsT2h: String = ""
  var userAvsT6h: String = ""
  var userAvsT12h: String = ""
  /**
    * 用户1小时、2小时、6小时、12小时阅读帖子占比
    */
  var userRead1Rate1h: String = ""
  var userRead1Rate2h: String = ""
  var userRead1Rate6h: String = ""
  var userRead1Rate12h: String = ""
  /**
    * 用户1小时、2小时、6小时、12小时阅读资讯占比
    */
  var userRead2Rate1h: String = ""
  var userRead2Rate2h: String = ""
  var userRead2Rate6h: String = ""
  var userRead2Rate12h: String = ""
  /**
    * 用户最近N条的item历史记录
    * 用户最近N条的类别历史记录
    * 用户最近N条的关键词历史记录
    * 用户最近N条的tag历史记录
    */
  var userHList: String = ""
  var userItemHistory: String = ""
  var userCateHistory: String = ""
  var userKeywordHistory: String = ""
  var userKeyword2History: String = ""
  var userTag1History: String = ""
  var userTag2History: String = ""
  var userTag3History: String = ""
  var userKs1History: String = ""
  var userKs2History: String = ""

  /** ============================Item features============================ */
  /**
    * 文本长度
    */
  var itemTexL: String = ""
  /**
    * 文本关键词个数
    */
  var itemKwN: String = ""
  /**
    * 标题长度
    */
  var itemTitL: String = ""
  /**
    * 标题关键词个数
    */
  var itemTwN: String = ""
  /**
    * 文本图片数
    */
  var itemImgN: String = ""
  /**
    * 内容类型 1：纯文本，2：图文混排，3：纯图片，4：纯视频，5：图片视频混排，11：帖子
    */
  var itemSubT: String = ""
  /**
    * 资讯采集来源
    */
  var itemSour: String = ""
  /**
    * 视频时长
    */
  var itemVidT: String = ""
  /**
    * 创建时间
    */
  var itemCreT: String = ""
  /**
    * 内容长度
    */
  var itemContentH: String = ""
  /**
    * 作者
    */
  var itemAuthor: String = ""
  var itemAuthorLevel: String = ""
  var itemAuthorScore: String = ""

  /**
    * 主题权重
    */
  var itemTtP: String = ""
  /**
    * 关键字权重
    */
  var itemKtW: String = ""
  var itemKtW2: String = ""

  /**
    * 关键字CTR
    */
  var itemKtctr: String = ""
  /**
    * 点击
    */
  var itemCliN_Inc: String = ""
  /**
    * 曝光
    */
  var itemShoN_Inc: String = ""
  /**
    * 回复
    */
  var itemRevi: String = ""
  /**
    * 收藏
    */
  var itemColN: String = ""
  /**
    * 分享
    */
  var itemShare: String = ""
  /**
    * 浏览评论次数
    */
  var itemVreN: String = ""
  /**
    * 评论点赞数
    */
  var itemLireN: String = ""
  /**
    * 举报数
    */
  var itemRepN: String = ""
  /**
    * 点击率
    */
  var itemCtr: String = ""
  /**
    * 修正点击率
    */
  var itemWilCtr: String = ""
  /**
    * 总浏览次数
    */
  var itemTimes: String = ""
  /**
    * 有效阅读次数
    */
  var itemEffTimes: String = ""
  /**
    * 有效阅读用户数
    */
  var itemEffUsers: String = ""
  /**
    * 浏览到底部次数
    */
  var itemView2BottomTimes: String = ""
  /**
    * 总浏览时长
    */
  var itemTotalTime: String = ""
  /**
    * 人均停留时长
    */
  var itemAvsT: String = ""
  /**
    * 浏览完成率
    */
  var itemFiR: String = ""
  /**
    * 阅读时长评分
    */
  var itemTimeScore: String = ""
  /**
    * 点击回复比
    */
  var itemCliRevR: String = ""
  /**
    * 点击收藏比
    */
  var itemCliColR: String = ""
  /**
    * 点击分享比
    */
  var itemCliShareR: String = ""
  /**
    * 点击浏览评论比
    */
  var itemCliVreR: String = ""
  /**
    * 点击回复点赞比
    */
  var itemCliLireR: String = ""
  /**
    * 点击举报比
    */
  var itemCliRepR: String = ""
  /**
    * 底部行为总数
    */
  var itemBotSum: String = ""
  /**
    * tag、关键词
    */
  var itemTagSvC4: String = ""
  var itemKtSvC2: String = ""
  var itemTsC4: String = ""
  var itemKsC4: String = ""
  /**
    * 一级、二级、三级tag，关键词v1和v2
    */
  var itemTag1: String = ""
  var itemTag2: String = ""
  var itemTag3: String = ""
  var itemKs1: String = ""
  var itemKs2: String = ""
  /**
    * 1天、3天、7天item点击数
    */
  var itemCliN_Inc1d: String = ""
  var itemCliN_Inc3d: String = ""
  var itemCliN_Inc7d: String = ""
  /**
    * 1天、3天、7天item点击UV数
    */
  var itemCliUV_Inc1d: String = ""
  var itemCliUV_Inc3d: String = ""
  var itemCliUV_Inc7d: String = ""
  /**
    * 1天、3天、7天item暴光数
    */
  var itemShoN_Inc1d: String = ""
  var itemShoN_Inc3d: String = ""
  var itemShoN_Inc7d: String = ""
  /**
    * 1天、3天、7天item暴光UV数
    */
  var itemShoUV_Inc1d: String = ""
  var itemShoUV_Inc3d: String = ""
  var itemShoUV_Inc7d: String = ""
  /**
    * 1天、3天、7天item点击率
    */
  var itemCtr1d: String = ""
  var itemCtr3d: String = ""
  var itemCtr7d: String = ""
  /**
    * 1天、3天、7天item转化率
    */
  var itemCtrUV1d: String = ""
  var itemCtrUV3d: String = ""
  var itemCtrUV7d: String = ""
  /**
    * 1天、3天、7天item浏览数
    */
  var itemTimes1d: String = ""
  var itemTimes3d: String = ""
  var itemTimes7d: String = ""
  /**
    * 1天、3天、7天item有效浏览数
    */
  var itemEffTimes1d: String = ""
  var itemEffTimes3d: String = ""
  var itemEffTimes7d: String = ""
  /**
    * 1天、3天、7天item有效浏览用户数
    */
  var itemEffUsers1d: String = ""
  var itemEffUsers3d: String = ""
  var itemEffUsers7d: String = ""
  /**
    * 1天、3天、7天item浏览到底部次数
    */
  var itemView2BottomTimes1d: String = ""
  var itemView2BottomTimes3d: String = ""
  var itemView2BottomTimes7d: String = ""
  /**
    * 1天、3天、7天item总停留时长
    */
  var itemTotalTime1d: String = ""
  var itemTotalTime3d: String = ""
  var itemTotalTime7d: String = ""
  /**
    * 1天、3天、7天item平均停留时长
    */
  var itemAvsT1d: String = ""
  var itemAvsT3d: String = ""
  var itemAvsT7d: String = ""
  /**
    * 1天、3天、7天item浏览完成率
    */
  var itemFiR1d: String = ""
  var itemFiR3d: String = ""
  var itemFiR7d: String = ""

  var itemValidReadR1d: String = ""
  var itemValidReadR3d: String = ""
  var itemValidReadR7d: String = ""

  var itemUVValidReadR1d: String = ""
  var itemUVValidReadR3d: String = ""
  var itemUVValidReadR7d: String = ""

  var itemValidRead2R1d: String = ""
  var itemValidRead2R3d: String = ""
  var itemValidRead2R7d: String = ""

  var itemUVValidRead2R1d: String = ""
  var itemUVValidRead2R3d: String = ""
  var itemUVValidRead2R7d: String = ""
  /**
    * 1小时、2小时、6小时、12小时item点击数
    */
  var itemCliN_Inc1h: String = ""
  var itemCliN_Inc2h: String = ""
  var itemCliN_Inc6h: String = ""
  var itemCliN_Inc12h: String = ""
  /**
    * 1小时、2小时、6小时、12小时item点击UV数
    */
  var itemCliUV_Inc1h: String = ""
  var itemCliUV_Inc2h: String = ""
  var itemCliUV_Inc6h: String = ""
  var itemCliUV_Inc12h: String = ""
  /**
    * 1小时、2小时、6小时、12小时item暴光数
    */
  var itemShoN_Inc1h: String = ""
  var itemShoN_Inc2h: String = ""
  var itemShoN_Inc6h: String = ""
  var itemShoN_Inc12h: String = ""
  /**
    * 1小时、2小时、6小时、12小时item暴光用户数
    */
  var itemShoUV_Inc1h: String = ""
  var itemShoUV_Inc2h: String = ""
  var itemShoUV_Inc6h: String = ""
  var itemShoUV_Inc12h: String = ""
  /**
    * 1小时、2小时、6小时、12小时item点击率
    */
  var itemCtr1h: String = ""
  var itemCtr2h: String = ""
  var itemCtr6h: String = ""
  var itemCtr12h: String = ""
  /**
    * 1小时、2小时、6小时、12小时item转化率
    */
  var itemCtrUV1h: String = ""
  var itemCtrUV2h: String = ""
  var itemCtrUV6h: String = ""
  var itemCtrUV12h: String = ""
  /**
    * 1小时、2小时、6小时、12小时item浏览次数
    */
  var itemTimes1h: String = ""
  var itemTimes2h: String = ""
  var itemTimes6h: String = ""
  var itemTimes12h: String = ""
  /**
    * 1小时、2小时、6小时、12小时item有效浏览数
    */
  var itemEffTimes1h: String = ""
  var itemEffTimes2h: String = ""
  var itemEffTimes6h: String = ""
  var itemEffTimes12h: String = ""
  /**
    * 1小时、2小时、6小时、12小时item有效浏览用户数
    */
  var itemEffUsers1h: String = ""
  var itemEffUsers2h: String = ""
  var itemEffUsers6h: String = ""
  var itemEffUsers12h: String = ""
  /**
    * 1小时、2小时、6小时、12小时浏览到底部次数
    */
  var itemView2BottomTimes1h: String = ""
  var itemView2BottomTimes2h: String = ""
  var itemView2BottomTimes6h: String = ""
  var itemView2BottomTimes12h: String = ""
  /**
    * 1小时、2小时、6小时、12小时浏览总时长
    */
  var itemTotalTime1h: String = ""
  var itemTotalTime2h: String = ""
  var itemTotalTime6h: String = ""
  var itemTotalTime12h: String = ""
  /**
    * 1小时、2小时、6小时、12小时平均浏览时长
    */
  var itemAvsT1h: String = ""
  var itemAvsT2h: String = ""
  var itemAvsT6h: String = ""
  var itemAvsT12h: String = ""
  /**
    * 1小时、2小时、6小时、12小时item浏览完成率
    */
  var itemFiR1h: String = ""
  var itemFiR2h: String = ""
  var itemFiR6h: String = ""
  var itemFiR12h: String = ""

  var itemValidReadR1h: String = ""
  var itemValidReadR2h: String = ""
  var itemValidReadR6h: String = ""
  var itemValidReadR12h: String = ""

  var itemUVValidReadR1h: String = ""
  var itemUVValidReadR2h: String = ""
  var itemUVValidReadR6h: String = ""
  var itemUVValidReadR12h: String = ""

  var itemValidRead2R1h: String = ""
  var itemValidRead2R2h: String = ""
  var itemValidRead2R6h: String = ""
  var itemValidRead2R12h: String = ""

  var itemUVValidRead2R1h: String = ""
  var itemUVValidRead2R2h: String = ""
  var itemUVValidRead2R6h: String = ""
  var itemUVValidRead2R12h: String = ""

  /** ============================Item User Cross features============================ */
  var crossTtP: String = ""
  var crossTtPW: String = ""
  var crossKtW: String = ""
  var crossKtWW: String = ""
  var crossTop3TtP: String = ""
  var crossTop6TtP: String = ""
  var crossKtW2: String = ""


  def label2String: String = {
    "1^" +
      label + "^" +
      userId + "^" +
      itemId + "^" +
      itemDataType + "^" +
      userRequestTime + "^" +
      userDayOrNight + "^" +
      itemAlgSource + "^" +
      userIP + "^" +
      itemFloor
  }

  def user2String: String = {
    "2^" +
      userId + "^" +
      userOt + "^" +
      userOs + "^" +
      userApn + "^" +
      userMode + "^" +
      userCity + "^" +
      userMarr + "^" +
      userGest + "^" +
      userAge + "^" +
      userBage + "^" +
      userAppV + "^" +
      userCliN_Inc + "^" +
      userShoN_Inc + "^" +
      userColN + "^" +
      userRevi + "^" +
      userRepN + "^" +
      userShare + "^" +
      userLireN + "^" +
      userVreN + "^" +
      userTtP + "^" +
      userKtW + "^" +
      userOsApn + "^" +
      userApnOt + "^" +
      userCtr + "^" +
      userTotalTime + "^" +
      userView2BottomTimes + "^" +
      userEffTimes + "^" +
      userBageAgeMinus + "^" +
      userGestMarr + "^" +
      userOsOt + "^" +
      userUa + "^" +
      userCliN_Inc1d + "^" +
      userCliN_Inc2d + "^" +
      userCliN_Inc3d + "^" +
      userCliN_Inc5d + "^" +
      userCliN_Inc7d + "^" +
      userShoN_Inc1d + "^" +
      userShoN_Inc2d + "^" +
      userShoN_Inc3d + "^" +
      userShoN_Inc5d + "^" +
      userShoN_Inc7d + "^" +
      userCtr1d + "^" +
      userCtr2d + "^" +
      userCtr3d + "^" +
      userCtr5d + "^" +
      userCtr7d + "^" +
      userEffTimes1d + "^" +
      userEffTimes2d + "^" +
      userEffTimes3d + "^" +
      userEffTimes5d + "^" +
      userEffTimes7d + "^" +
      userView2BottomTimes1d + "^" +
      userView2BottomTimes2d + "^" +
      userView2BottomTimes3d + "^" +
      userView2BottomTimes5d + "^" +
      userView2BottomTimes7d + "^" +
      userTotalTime1d + "^" +
      userTotalTime2d + "^" +
      userTotalTime3d + "^" +
      userTotalTime5d + "^" +
      userTotalTime7d + "^" +
      userAvsT1d + "^" +
      userAvsT2d + "^" +
      userAvsT3d + "^" +
      userAvsT5d + "^" +
      userAvsT7d + "^" +
      userRead1Rate1d + "^" +
      userRead1Rate2d + "^" +
      userRead1Rate3d + "^" +
      userRead1Rate5d + "^" +
      userRead1Rate7d + "^" +
      userRead2Rate1d + "^" +
      userRead2Rate2d + "^" +
      userRead2Rate3d + "^" +
      userRead2Rate5d + "^" +
      userRead2Rate7d + "^" +
      userFloorP + "^" +
      userNewsTypeP + "^" +
      userTagPC4 + "^" +
      userTagPC2 + "^" +
      userFloorPD + "^" +
      userNewsTypePD + "^" +
      userTagPDC4 + "^" +
      userTagPDC2
  }

  def item2String: String = {
    "3^" +
      itemId + "^" +
      itemDataType + "^" +
      itemTexL + "^" +
      itemKwN + "^" +
      itemTitL + "^" +
      itemTwN + "^" +
      itemImgN + "^" +
      itemSubT + "^" +
      itemSour + "^" +
      itemVidT + "^" +
      itemCreT + "^" +
      itemContentH + "^" +
      itemAuthor + "^" +
      itemTtP + "^" +
      itemKtW + "^" +
      itemKtctr + "^" +
      itemCliN_Inc + "^" +
      itemShoN_Inc + "^" +
      itemRevi + "^" +
      itemColN + "^" +
      itemShare + "^" +
      itemVreN + "^" +
      itemLireN + "^" +
      itemRepN + "^" +
      itemCtr + "^" +
      itemWilCtr + "^" +
      itemTimes + "^" +
      itemEffTimes + "^" +
      itemEffUsers + "^" +
      itemView2BottomTimes + "^" +
      itemTotalTime + "^" +
      itemAvsT + "^" +
      itemFiR + "^" +
      itemTimeScore + "^" +
      itemCliRevR + "^" +
      itemCliColR + "^" +
      itemCliShareR + "^" +
      itemCliVreR + "^" +
      itemCliLireR + "^" +
      itemCliRepR + "^" +
      itemBotSum + "^" +
      itemCliN_Inc1d + "^" +
      itemCliN_Inc3d + "^" +
      itemCliN_Inc7d + "^" +
      itemCliUV_Inc1d + "^" +
      itemCliUV_Inc3d + "^" +
      itemCliUV_Inc7d + "^" +
      itemShoN_Inc1d + "^" +
      itemShoN_Inc3d + "^" +
      itemShoN_Inc7d + "^" +
      itemShoUV_Inc1d + "^" +
      itemShoUV_Inc3d + "^" +
      itemShoUV_Inc7d + "^" +
      itemCtr1d + "^" +
      itemCtr3d + "^" +
      itemCtr7d + "^" +
      itemCtrUV1d + "^" +
      itemCtrUV3d + "^" +
      itemCtrUV7d + "^" +
      itemTimes1d + "^" +
      itemTimes3d + "^" +
      itemTimes7d + "^" +
      itemEffTimes1d + "^" +
      itemEffTimes3d + "^" +
      itemEffTimes7d + "^" +
      itemEffUsers1d + "^" +
      itemEffUsers3d + "^" +
      itemEffUsers7d + "^" +
      itemView2BottomTimes1d + "^" +
      itemView2BottomTimes3d + "^" +
      itemView2BottomTimes7d + "^" +
      itemTotalTime1d + "^" +
      itemTotalTime3d + "^" +
      itemTotalTime7d + "^" +
      itemAvsT1d + "^" +
      itemAvsT3d + "^" +
      itemAvsT7d + "^" +
      itemFiR1d + "^" +
      itemFiR3d + "^" +
      itemFiR7d + "^" +
      itemValidReadR1d + "^" +
      itemValidReadR3d + "^" +
      itemValidReadR7d + "^" +
      itemUVValidReadR1d + "^" +
      itemUVValidReadR3d + "^" +
      itemUVValidReadR7d + "^" +
      itemValidRead2R1d + "^" +
      itemValidRead2R3d + "^" +
      itemValidRead2R7d + "^" +
      itemUVValidRead2R1d + "^" +
      itemUVValidRead2R3d + "^" +
      itemUVValidRead2R7d + "^" +
      itemTagSvC4 + "^" +
      itemKtSvC2 + "^" +
      itemTsC4 + "^" +
      itemKsC4
  }

  def label2StringDIN: String = {
    "1^" +
      label + "^" +
      userId + "^" +
      itemId + "^" +
      itemDataType + "^" +
      userRequestTime + "^" +
      itemAlgSource + "^" +
      userIP
  }

  def user2StringDIN: String = {
    "2^" +
      userId + "^" +
      userOt + "^" +
      userOs + "^" +
      userApn + "^" +
      userMode + "^" +
      userCity + "^" +
      userMarr + "^" +
      userGest + "^" +
      userAge + "^" +
      userBage + "^" +
      userAppV + "^" +
      userCliN_Inc + "^" +
      userShoN_Inc + "^" +
      userColN + "^" +
      userRevi + "^" +
      userRepN + "^" +
      userShare + "^" +
      userLireN + "^" +
      userVreN + "^" +
      userTotalTime + "^" +
      userView2BottomTimes + "^" +
      userEffTimes + "^" +
      userUa + "^" +
      userHList
  }

  def item2StringDIN: String = {
    "3^" +
      itemId + "^" +
      itemDataType + "^" +
      itemTexL + "^" +
      itemKwN + "^" +
      itemTitL + "^" +
      itemTwN + "^" +
      itemImgN + "^" +
      itemSubT + "^" +
      itemSour + "^" +
      itemCreT + "^" +
      itemAuthor + "^" +
      itemTtP + "^" +
      itemKtW + "^" +
      itemCliN_Inc + "^" +
      itemShoN_Inc + "^" +
      itemRevi + "^" +
      itemColN + "^" +
      itemShare + "^" +
      itemVreN + "^" +
      itemLireN + "^" +
      itemRepN + "^" +
      itemEffUsers + "^" +
      itemView2BottomTimes + "^" +
      itemTotalTime + "^" +
      itemKtW2 + "^" +
      itemTag1 + "^" +
      itemTag2 + "^" +
      itemTag3 + "^" +
      itemKs1 + "^" +
      itemKs2
  }
}

object CommonLabelPoint2 {

  def toObject(str: String): CommonLabelPoint2 = {
    var lb = new CommonLabelPoint2
    val array = str.split("\\^")
    val size = array.length
    if (size == 11 || size == 78 || size == 94) {
      lb.batchNo = array(0)
      lb.logType = array(1)
      if (lb.logType == "1") {
        lb.label = array(2)
        lb.userId = array(3)
        lb.itemId = array(4)
        lb.itemDataType = array(5)
        lb.userRequestTime = array(6)
        lb.userDayOrNight = array(7)
        lb.itemAlgSource = array(8)
        lb.userIP = array(9)
        lb.itemFloor = array(10)
      }
      else if (lb.logType == "2") {
        lb.userId = array(2)
        lb.userOt = array(3)
        lb.userOs = array(4)
        lb.userApn = array(5)
        lb.userMode = array(6)
        lb.userCity = array(7)
        lb.userMarr = array(8)
        lb.userGest = array(9)
        lb.userAge = array(10)
        lb.userBage = array(11)
        lb.userAppV = array(12)
        lb.userCliN_Inc = array(13)
        lb.userShoN_Inc = array(14)
        lb.userColN = array(15)
        lb.userRevi = array(16)
        lb.userRepN = array(17)
        lb.userShare = array(18)
        lb.userLireN = array(19)
        lb.userVreN = array(20)
        lb.userTtP = array(21)
        lb.userKtW = array(22)
        lb.userOsApn = array(23)
        lb.userApnOt = array(24)
        lb.userCtr = array(25)
        lb.userTotalTime = array(26)
        lb.userView2BottomTimes = array(27)
        lb.userEffTimes = array(28)
        lb.userBageAgeMinus = array(29)
        lb.userGestMarr = array(30)
        lb.userOsOt = array(31)
        lb.userUa = array(32)
        lb.userCliN_Inc1d = array(33)
        lb.userCliN_Inc2d = array(34)
        lb.userCliN_Inc3d = array(35)
        lb.userCliN_Inc5d = array(36)
        lb.userCliN_Inc7d = array(37)
        lb.userShoN_Inc1d = array(38)
        lb.userShoN_Inc2d = array(39)
        lb.userShoN_Inc3d = array(40)
        lb.userShoN_Inc5d = array(41)
        lb.userShoN_Inc7d = array(42)
        lb.userCtr1d = array(43)
        lb.userCtr2d = array(44)
        lb.userCtr3d = array(45)
        lb.userCtr5d = array(46)
        lb.userCtr7d = array(47)
        lb.userEffTimes1d = array(48)
        lb.userEffTimes2d = array(49)
        lb.userEffTimes3d = array(50)
        lb.userEffTimes5d = array(51)
        lb.userEffTimes7d = array(52)
        lb.userView2BottomTimes1d = array(53)
        lb.userView2BottomTimes2d = array(54)
        lb.userView2BottomTimes3d = array(55)
        lb.userView2BottomTimes5d = array(56)
        lb.userView2BottomTimes7d = array(57)
        lb.userTotalTime1d = array(58)
        lb.userTotalTime2d = array(59)
        lb.userTotalTime3d = array(60)
        lb.userTotalTime5d = array(61)
        lb.userTotalTime7d = array(62)
        lb.userAvsT1d = array(63)
        lb.userAvsT2d = array(64)
        lb.userAvsT3d = array(65)
        lb.userAvsT5d = array(66)
        lb.userAvsT7d = array(67)
        lb.userRead1Rate1d = array(68)
        lb.userRead1Rate2d = array(69)
        lb.userRead1Rate3d = array(70)
        lb.userRead1Rate5d = array(71)
        lb.userRead1Rate7d = array(72)
        lb.userRead2Rate1d = array(73)
        lb.userRead2Rate2d = array(74)
        lb.userRead2Rate3d = array(75)
        lb.userRead2Rate5d = array(76)
        lb.userRead2Rate7d = array(77)
        lb.userCliN_Inc1h = array(78)
        lb.userCliN_Inc2h = array(79)
        lb.userCliN_Inc6h = array(80)
        lb.userCliN_Inc12h = array(81)
        lb.userShoN_Inc1h = array(82)
        lb.userShoN_Inc2h = array(83)
        lb.userShoN_Inc6h = array(84)
        lb.userShoN_Inc12h = array(85)
        lb.userCtr1h = array(86)
        lb.userCtr2h = array(87)
        lb.userCtr6h = array(88)
        lb.userCtr12h = array(89)
        lb.userEffTimes1h = array(90)
        lb.userEffTimes2h = array(91)
        lb.userEffTimes6h = array(92)
        lb.userEffTimes12h = array(93)
        lb.userView2BottomTimes1h = array(94)
        lb.userView2BottomTimes2h = array(95)
        lb.userView2BottomTimes6h = array(96)
        lb.userView2BottomTimes12h = array(97)
        lb.userTotalTime1h = array(98)
        lb.userTotalTime2h = array(99)
        lb.userTotalTime6h = array(100)
        lb.userTotalTime12h = array(101)
        lb.userAvsT1h = array(102)
        lb.userAvsT2h = array(103)
        lb.userAvsT6h = array(104)
        lb.userAvsT12h = array(105)
        lb.userRead1Rate1h = array(106)
        lb.userRead1Rate2h = array(107)
        lb.userRead1Rate6h = array(108)
        lb.userRead1Rate12h = array(109)
        lb.userRead2Rate1h = array(110)
        lb.userRead2Rate2h = array(111)
        lb.userRead2Rate6h = array(112)
        lb.userRead2Rate12h = array(113)
      }
      else if (lb.logType == "3") {
        lb.itemId = array(2)
        lb.itemDataType = array(3)
        lb.itemTexL = array(4)
        lb.itemKwN = array(5)
        lb.itemTitL = array(6)
        lb.itemTwN = array(7)
        lb.itemImgN = array(8)
        lb.itemSubT = array(9)
        lb.itemSour = array(10)
        lb.itemVidT = array(11)
        lb.itemCreT = array(12)
        lb.itemContentH = array(13)
        lb.itemAuthor = array(14)
        lb.itemTtP = array(15)
        lb.itemKtW = array(16)
        lb.itemKtctr = array(17)
        lb.itemCliN_Inc = array(18)
        lb.itemShoN_Inc = array(19)
        lb.itemRevi = array(20)
        lb.itemColN = array(21)
        lb.itemShare = array(22)
        lb.itemVreN = array(23)
        lb.itemLireN = array(24)
        lb.itemRepN = array(25)
        lb.itemCtr = array(26)
        lb.itemWilCtr = array(27)
        lb.itemTimes = array(28)
        lb.itemEffTimes = array(29)
        lb.itemEffUsers = array(30)
        lb.itemView2BottomTimes = array(31)
        lb.itemTotalTime = array(32)
        lb.itemAvsT = array(33)
        lb.itemFiR = array(34)
        lb.itemTimeScore = array(35)
        lb.itemCliRevR = array(36)
        lb.itemCliColR = array(37)
        lb.itemCliShareR = array(38)
        lb.itemCliVreR = array(39)
        lb.itemCliLireR = array(40)
        lb.itemCliRepR = array(41)
        lb.itemBotSum = array(42)
        lb.itemCliN_Inc1d = array(43)
        lb.itemCliN_Inc3d = array(44)
        lb.itemCliN_Inc7d = array(45)
        lb.itemCliUV_Inc1d = array(46)
        lb.itemCliUV_Inc3d = array(47)
        lb.itemCliUV_Inc7d = array(48)
        lb.itemShoN_Inc1d = array(49)
        lb.itemShoN_Inc3d = array(50)
        lb.itemShoN_Inc7d = array(51)
        lb.itemShoUV_Inc1d = array(52)
        lb.itemShoUV_Inc3d = array(53)
        lb.itemShoUV_Inc7d = array(54)
        lb.itemCtr1d = array(55)
        lb.itemCtr3d = array(56)
        lb.itemCtr7d = array(57)
        lb.itemCtrUV1d = array(58)
        lb.itemCtrUV3d = array(59)
        lb.itemCtrUV7d = array(60)
        lb.itemTimes1d = array(61)
        lb.itemTimes3d = array(62)
        lb.itemTimes7d = array(63)
        lb.itemEffTimes1d = array(64)
        lb.itemEffTimes3d = array(65)
        lb.itemEffTimes7d = array(66)
        lb.itemEffUsers1d = array(67)
        lb.itemEffUsers3d = array(68)
        lb.itemEffUsers7d = array(69)
        lb.itemView2BottomTimes1d = array(70)
        lb.itemView2BottomTimes3d = array(71)
        lb.itemView2BottomTimes7d = array(72)
        lb.itemTotalTime1d = array(73)
        lb.itemTotalTime3d = array(74)
        lb.itemTotalTime7d = array(75)
        lb.itemAvsT1d = array(76)
        lb.itemAvsT3d = array(77)
        lb.itemAvsT7d = array(78)
        lb.itemFiR1d = array(79)
        lb.itemFiR3d = array(80)
        lb.itemFiR7d = array(81)
        lb.itemValidReadR1d = array(82)
        lb.itemValidReadR3d = array(83)
        lb.itemValidReadR7d = array(84)
        lb.itemUVValidReadR1d = array(85)
        lb.itemUVValidReadR3d = array(86)
        lb.itemUVValidReadR7d = array(87)
        lb.itemValidRead2R1d = array(88)
        lb.itemValidRead2R3d = array(89)
        lb.itemValidRead2R7d = array(90)
        lb.itemUVValidRead2R1d = array(91)
        lb.itemUVValidRead2R3d = array(92)
        lb.itemUVValidRead2R7d = array(93)
        lb.itemCliN_Inc1h = array(94)
        lb.itemCliN_Inc2h = array(95)
        lb.itemCliN_Inc6h = array(96)
        lb.itemCliN_Inc12h = array(97)
        lb.itemCliUV_Inc1h = array(98)
        lb.itemCliUV_Inc2h = array(99)
        lb.itemCliUV_Inc6h = array(100)
        lb.itemCliUV_Inc12h = array(101)
        lb.itemShoN_Inc1h = array(102)
        lb.itemShoN_Inc2h = array(103)
        lb.itemShoN_Inc6h = array(104)
        lb.itemShoN_Inc12h = array(105)
        lb.itemShoUV_Inc1h = array(106)
        lb.itemShoUV_Inc2h = array(107)
        lb.itemShoUV_Inc6h = array(108)
        lb.itemShoUV_Inc12h = array(109)
        lb.itemCtr1h = array(110)
        lb.itemCtr2h = array(111)
        lb.itemCtr6h = array(112)
        lb.itemCtr12h = array(113)
        lb.itemCtrUV1h = array(114)
        lb.itemCtrUV2h = array(115)
        lb.itemCtrUV6h = array(116)
        lb.itemCtrUV12h = array(117)
        lb.itemTimes1h = array(118)
        lb.itemTimes2h = array(119)
        lb.itemTimes6h = array(120)
        lb.itemTimes12h = array(121)
        lb.itemEffTimes1h = array(122)
        lb.itemEffTimes2h = array(123)
        lb.itemEffTimes6h = array(124)
        lb.itemEffTimes12h = array(125)
        lb.itemEffUsers1h = array(126)
        lb.itemEffUsers2h = array(127)
        lb.itemEffUsers6h = array(128)
        lb.itemEffUsers12h = array(129)
        lb.itemView2BottomTimes1h = array(130)
        lb.itemView2BottomTimes2h = array(131)
        lb.itemView2BottomTimes6h = array(132)
        lb.itemView2BottomTimes12h = array(133)
        lb.itemTotalTime1h = array(134)
        lb.itemTotalTime2h = array(135)
        lb.itemTotalTime6h = array(136)
        lb.itemTotalTime12h = array(137)
        lb.itemAvsT1h = array(138)
        lb.itemAvsT2h = array(139)
        lb.itemAvsT6h = array(140)
        lb.itemAvsT12h = array(141)
        lb.itemFiR1h = array(142)
        lb.itemFiR2h = array(143)
        lb.itemFiR6h = array(144)
        lb.itemFiR12h = array(145)
        lb.itemValidReadR1h = array(146)
        lb.itemValidReadR2h = array(147)
        lb.itemValidReadR6h = array(148)
        lb.itemValidReadR12h = array(149)
        lb.itemUVValidReadR1h = array(150)
        lb.itemUVValidReadR2h = array(151)
        lb.itemUVValidReadR6h = array(152)
        lb.itemUVValidReadR12h = array(153)
        lb.itemValidRead2R1h = array(154)
        lb.itemValidRead2R2h = array(155)
        lb.itemValidRead2R6h = array(156)
        lb.itemValidRead2R12h = array(157)
        lb.itemUVValidRead2R1h = array(158)
        lb.itemUVValidRead2R2h = array(159)
        lb.itemUVValidRead2R6h = array(160)
        lb.itemUVValidRead2R12h = array(161)
      }
    }
    else {
      lb = null
    }
    lb
  }

  def toObjectForDIN(str: String): CommonLabelPoint2 = {
    var lb = new CommonLabelPoint2
    val array = str.split("\\^")
    val size = array.length
    if (size == 9 || size == 26 || size == 32) {
      lb.batchNo = array(0)
      lb.logType = array(1)
      if (lb.logType == "1") {
        lb.label = array(2)
        lb.userId = array(3)
        lb.itemId = array(4)
        lb.itemDataType = array(5)
        lb.userRequestTime = array(6)
        lb.itemAlgSource = array(7)
        lb.userIP = array(8)
      }
      else if (lb.logType == "2") {
        lb.userId = array(2)
        lb.userOt = array(3)
        lb.userOs = array(4)
        lb.userApn = array(5)
        lb.userMode = array(6)
        lb.userCity = array(7)
        lb.userMarr = array(8)
        lb.userGest = array(9)
        lb.userAge = array(10)
        lb.userBage = array(11)
        lb.userAppV = array(12)
        lb.userCliN_Inc = array(13)
        lb.userShoN_Inc = array(14)
        lb.userColN = array(15)
        lb.userRevi = array(16)
        lb.userRepN = array(17)
        lb.userShare = array(18)
        lb.userLireN = array(19)
        lb.userVreN = array(20)
        lb.userTotalTime = array(21)
        lb.userView2BottomTimes = array(22)
        lb.userEffTimes = array(23)
        lb.userUa = array(24)
        lb.userHList = array(25)

      }
      else if (lb.logType == "3") {
        lb.itemId = array(2)
        lb.itemDataType = array(3)
        lb.itemTexL = array(4)
        lb.itemKwN = array(5)
        lb.itemTitL = array(6)
        lb.itemTwN = array(7)
        lb.itemImgN = array(8)
        lb.itemSubT = array(9)
        lb.itemSour = array(10)
        lb.itemCreT = array(11)
        lb.itemAuthor = array(12)
        lb.itemTtP = array(13)
        lb.itemKtW = array(14)
        lb.itemCliN_Inc = array(15)
        lb.itemShoN_Inc = array(16)
        lb.itemRevi = array(17)
        lb.itemColN = array(18)
        lb.itemShare = array(19)
        lb.itemVreN = array(20)
        lb.itemLireN = array(21)
        lb.itemRepN = array(22)
        lb.itemEffUsers = array(23)
        lb.itemView2BottomTimes = array(24)
        lb.itemTotalTime = array(25)
        lb.itemKtW2 = array(26)
        lb.itemTag1 = array(27)
        lb.itemTag2 = array(28)
        lb.itemTag3 = array(29)
        lb.itemKs1 = array(30)
        lb.itemKs2 = array(31)

      }
    }
    else {
      lb = null
    }
    lb
  }

  def copyUser2Lb(lb: CommonLabelPoint2, user: CommonLabelPoint2) = {
    lb.userOt = user.userOt
    lb.userOs = user.userOs
    lb.userApn = user.userApn
    lb.userMode = user.userMode
    lb.userCity = user.userCity
    lb.userMarr = user.userMarr
    lb.userGest = user.userGest
    lb.userAge = user.userAge
    lb.userBage = user.userBage
    lb.userAppV = user.userAppV
    lb.userCliN_Inc = user.userCliN_Inc
    lb.userShoN_Inc = user.userShoN_Inc
    lb.userColN = user.userColN
    lb.userRevi = user.userRevi
    lb.userRepN = user.userRepN
    lb.userShare = user.userShare
    lb.userLireN = user.userLireN
    lb.userVreN = user.userVreN
    lb.userTtP = user.userTtP
    lb.userKtW = user.userKtW
    lb.userOsApn = user.userOsApn
    lb.userApnOt = user.userApnOt
    lb.userCtr = user.userCtr
    lb.userTotalTime = user.userTotalTime
    lb.userView2BottomTimes = user.userView2BottomTimes
    lb.userEffTimes = user.userEffTimes
    lb.userBageAgeMinus = user.userBageAgeMinus
    lb.userGestMarr = user.userGestMarr
    lb.userOsOt = user.userOsOt
    lb.userUa = user.userUa
    lb.userCliN_Inc1d = user.userCliN_Inc1d
    lb.userCliN_Inc2d = user.userCliN_Inc2d
    lb.userCliN_Inc3d = user.userCliN_Inc3d
    lb.userCliN_Inc5d = user.userCliN_Inc5d
    lb.userCliN_Inc7d = user.userCliN_Inc7d
    lb.userShoN_Inc1d = user.userShoN_Inc1d
    lb.userShoN_Inc2d = user.userShoN_Inc2d
    lb.userShoN_Inc3d = user.userShoN_Inc3d
    lb.userShoN_Inc5d = user.userShoN_Inc5d
    lb.userShoN_Inc7d = user.userShoN_Inc7d
    lb.userCtr1d = user.userCtr1d
    lb.userCtr2d = user.userCtr2d
    lb.userCtr3d = user.userCtr3d
    lb.userCtr5d = user.userCtr5d
    lb.userCtr7d = user.userCtr7d
    lb.userEffTimes1d = user.userEffTimes1d
    lb.userEffTimes2d = user.userEffTimes2d
    lb.userEffTimes3d = user.userEffTimes3d
    lb.userEffTimes5d = user.userEffTimes5d
    lb.userEffTimes7d = user.userEffTimes7d
    lb.userView2BottomTimes1d = user.userView2BottomTimes1d
    lb.userView2BottomTimes2d = user.userView2BottomTimes2d
    lb.userView2BottomTimes3d = user.userView2BottomTimes3d
    lb.userView2BottomTimes5d = user.userView2BottomTimes5d
    lb.userView2BottomTimes7d = user.userView2BottomTimes7d
    lb.userTotalTime1d = user.userTotalTime1d
    lb.userTotalTime2d = user.userTotalTime2d
    lb.userTotalTime3d = user.userTotalTime3d
    lb.userTotalTime5d = user.userTotalTime5d
    lb.userTotalTime7d = user.userTotalTime7d
    lb.userAvsT1d = user.userAvsT1d
    lb.userAvsT2d = user.userAvsT2d
    lb.userAvsT3d = user.userAvsT3d
    lb.userAvsT5d = user.userAvsT5d
    lb.userAvsT7d = user.userAvsT7d
    lb.userRead1Rate1d = user.userRead1Rate1d
    lb.userRead1Rate2d = user.userRead1Rate2d
    lb.userRead1Rate3d = user.userRead1Rate3d
    lb.userRead1Rate5d = user.userRead1Rate5d
    lb.userRead1Rate7d = user.userRead1Rate7d
    lb.userRead2Rate1d = user.userRead2Rate1d
    lb.userRead2Rate2d = user.userRead2Rate2d
    lb.userRead2Rate3d = user.userRead2Rate3d
    lb.userRead2Rate5d = user.userRead2Rate5d
    lb.userRead2Rate7d = user.userRead2Rate7d
    lb.userCliN_Inc1h = user.userCliN_Inc1h
    lb.userCliN_Inc2h = user.userCliN_Inc2h
    lb.userCliN_Inc6h = user.userCliN_Inc6h
    lb.userCliN_Inc12h = user.userCliN_Inc12h
    lb.userShoN_Inc1h = user.userShoN_Inc1h
    lb.userShoN_Inc2h = user.userShoN_Inc2h
    lb.userShoN_Inc6h = user.userShoN_Inc6h
    lb.userShoN_Inc12h = user.userShoN_Inc12h
    lb.userCtr1h = user.userCtr1h
    lb.userCtr2h = user.userCtr2h
    lb.userCtr6h = user.userCtr6h
    lb.userCtr12h = user.userCtr12h
    lb.userEffTimes1h = user.userEffTimes1h
    lb.userEffTimes2h = user.userEffTimes2h
    lb.userEffTimes6h = user.userEffTimes6h
    lb.userEffTimes12h = user.userEffTimes12h
    lb.userView2BottomTimes1h = user.userView2BottomTimes1h
    lb.userView2BottomTimes2h = user.userView2BottomTimes2h
    lb.userView2BottomTimes6h = user.userView2BottomTimes6h
    lb.userView2BottomTimes12h = user.userView2BottomTimes12h
    lb.userTotalTime1h = user.userTotalTime1h
    lb.userTotalTime2h = user.userTotalTime2h
    lb.userTotalTime6h = user.userTotalTime6h
    lb.userTotalTime12h = user.userTotalTime12h
    lb.userAvsT1h = user.userAvsT1h
    lb.userAvsT2h = user.userAvsT2h
    lb.userAvsT6h = user.userAvsT6h
    lb.userAvsT12h = user.userAvsT12h
    lb.userRead1Rate1h = user.userRead1Rate1h
    lb.userRead1Rate2h = user.userRead1Rate2h
    lb.userRead1Rate6h = user.userRead1Rate6h
    lb.userRead1Rate12h = user.userRead1Rate12h
    lb.userRead2Rate1h = user.userRead2Rate1h
    lb.userRead2Rate2h = user.userRead2Rate2h
    lb.userRead2Rate6h = user.userRead2Rate6h
    lb.userRead2Rate12h = user.userRead2Rate12h
    lb
  }

  def copyUser2LbForDIN(lb: CommonLabelPoint2, user: CommonLabelPoint2) = {
    lb.userOt = user.userOt
    lb.userOs = user.userOs
    lb.userApn = user.userApn
    lb.userMode = user.userMode
    lb.userCity = user.userCity
    lb.userMarr = user.userMarr
    lb.userGest = user.userGest
    lb.userAge = user.userAge
    lb.userBage = user.userBage
    lb.userAppV = user.userAppV
    lb.userCliN_Inc = user.userCliN_Inc
    lb.userShoN_Inc = user.userShoN_Inc
    lb.userColN = user.userColN
    lb.userRevi = user.userRevi
    lb.userRepN = user.userRepN
    lb.userShare = user.userShare
    lb.userLireN = user.userLireN
    lb.userVreN = user.userVreN
    lb.userTotalTime = user.userTotalTime
    lb.userView2BottomTimes = user.userView2BottomTimes
    lb.userEffTimes = user.userEffTimes
    lb.userUa = user.userUa
    lb.userHList = user.userHList

    lb
  }

  def copyItem2Lb(lb: CommonLabelPoint2, item: CommonLabelPoint2) = {
    lb.itemTexL = item.itemTexL
    lb.itemKwN = item.itemKwN
    lb.itemTitL = item.itemTitL
    lb.itemTwN = item.itemTwN
    lb.itemImgN = item.itemImgN
    lb.itemSubT = item.itemSubT
    lb.itemSour = item.itemSour
    lb.itemVidT = item.itemVidT
    lb.itemCreT = item.itemCreT
    lb.itemContentH = item.itemContentH
    lb.itemAuthor = item.itemAuthor
    lb.itemTtP = item.itemTtP
    lb.itemKtW = item.itemKtW
    lb.itemKtctr = item.itemKtctr
    lb.itemCliN_Inc = item.itemCliN_Inc
    lb.itemShoN_Inc = item.itemShoN_Inc
    lb.itemRevi = item.itemRevi
    lb.itemColN = item.itemColN
    lb.itemShare = item.itemShare
    lb.itemVreN = item.itemVreN
    lb.itemLireN = item.itemLireN
    lb.itemRepN = item.itemRepN
    lb.itemCtr = item.itemCtr
    lb.itemWilCtr = item.itemWilCtr
    lb.itemTimes = item.itemTimes
    lb.itemEffTimes = item.itemEffTimes
    lb.itemEffUsers = item.itemEffUsers
    lb.itemView2BottomTimes = item.itemView2BottomTimes
    lb.itemTotalTime = item.itemTotalTime
    lb.itemAvsT = item.itemAvsT
    lb.itemFiR = item.itemFiR
    lb.itemTimeScore = item.itemTimeScore
    lb.itemCliRevR = item.itemCliRevR
    lb.itemCliColR = item.itemCliColR
    lb.itemCliShareR = item.itemCliShareR
    lb.itemCliVreR = item.itemCliVreR
    lb.itemCliLireR = item.itemCliLireR
    lb.itemCliRepR = item.itemCliRepR
    lb.itemBotSum = item.itemBotSum
    lb.itemCliN_Inc1d = item.itemCliN_Inc1d
    lb.itemCliN_Inc3d = item.itemCliN_Inc3d
    lb.itemCliN_Inc7d = item.itemCliN_Inc7d
    lb.itemCliUV_Inc1d = item.itemCliUV_Inc1d
    lb.itemCliUV_Inc3d = item.itemCliUV_Inc3d
    lb.itemCliUV_Inc7d = item.itemCliUV_Inc7d
    lb.itemShoN_Inc1d = item.itemShoN_Inc1d
    lb.itemShoN_Inc3d = item.itemShoN_Inc3d
    lb.itemShoN_Inc7d = item.itemShoN_Inc7d
    lb.itemShoUV_Inc1d = item.itemShoUV_Inc1d
    lb.itemShoUV_Inc3d = item.itemShoUV_Inc3d
    lb.itemShoUV_Inc7d = item.itemShoUV_Inc7d
    lb.itemCtr1d = item.itemCtr1d
    lb.itemCtr3d = item.itemCtr3d
    lb.itemCtr7d = item.itemCtr7d
    lb.itemCtrUV1d = item.itemCtrUV1d
    lb.itemCtrUV3d = item.itemCtrUV3d
    lb.itemCtrUV7d = item.itemCtrUV7d
    lb.itemTimes1d = item.itemTimes1d
    lb.itemTimes3d = item.itemTimes3d
    lb.itemTimes7d = item.itemTimes7d
    lb.itemEffTimes1d = item.itemEffTimes1d
    lb.itemEffTimes3d = item.itemEffTimes3d
    lb.itemEffTimes7d = item.itemEffTimes7d
    lb.itemEffUsers1d = item.itemEffUsers1d
    lb.itemEffUsers3d = item.itemEffUsers3d
    lb.itemEffUsers7d = item.itemEffUsers7d
    lb.itemView2BottomTimes1d = item.itemView2BottomTimes1d
    lb.itemView2BottomTimes3d = item.itemView2BottomTimes3d
    lb.itemView2BottomTimes7d = item.itemView2BottomTimes7d
    lb.itemTotalTime1d = item.itemTotalTime1d
    lb.itemTotalTime3d = item.itemTotalTime3d
    lb.itemTotalTime7d = item.itemTotalTime7d
    lb.itemAvsT1d = item.itemAvsT1d
    lb.itemAvsT3d = item.itemAvsT3d
    lb.itemAvsT7d = item.itemAvsT7d
    lb.itemFiR1d = item.itemFiR1d
    lb.itemFiR3d = item.itemFiR3d
    lb.itemFiR7d = item.itemFiR7d
    lb.itemValidReadR1d = item.itemValidReadR1d
    lb.itemValidReadR3d = item.itemValidReadR3d
    lb.itemValidReadR7d = item.itemValidReadR7d
    lb.itemUVValidReadR1d = item.itemUVValidReadR1d
    lb.itemUVValidReadR3d = item.itemUVValidReadR3d
    lb.itemUVValidReadR7d = item.itemUVValidReadR7d
    lb.itemValidRead2R1d = item.itemValidRead2R1d
    lb.itemValidRead2R3d = item.itemValidRead2R3d
    lb.itemValidRead2R7d = item.itemValidRead2R7d
    lb.itemUVValidRead2R1d = item.itemUVValidRead2R1d
    lb.itemUVValidRead2R3d = item.itemUVValidRead2R3d
    lb.itemUVValidRead2R7d = item.itemUVValidRead2R7d
    lb.itemCliN_Inc1h = item.itemCliN_Inc1h
    lb.itemCliN_Inc2h = item.itemCliN_Inc2h
    lb.itemCliN_Inc6h = item.itemCliN_Inc6h
    lb.itemCliN_Inc12h = item.itemCliN_Inc12h
    lb.itemCliUV_Inc1h = item.itemCliUV_Inc1h
    lb.itemCliUV_Inc2h = item.itemCliUV_Inc2h
    lb.itemCliUV_Inc6h = item.itemCliUV_Inc6h
    lb.itemCliUV_Inc12h = item.itemCliUV_Inc12h
    lb.itemShoN_Inc1h = item.itemShoN_Inc1h
    lb.itemShoN_Inc2h = item.itemShoN_Inc2h
    lb.itemShoN_Inc6h = item.itemShoN_Inc6h
    lb.itemShoN_Inc12h = item.itemShoN_Inc12h
    lb.itemShoUV_Inc1h = item.itemShoUV_Inc1h
    lb.itemShoUV_Inc2h = item.itemShoUV_Inc2h
    lb.itemShoUV_Inc6h = item.itemShoUV_Inc6h
    lb.itemShoUV_Inc12h = item.itemShoUV_Inc12h
    lb.itemCtr1h = item.itemCtr1h
    lb.itemCtr2h = item.itemCtr2h
    lb.itemCtr6h = item.itemCtr6h
    lb.itemCtr12h = item.itemCtr12h
    lb.itemCtrUV1h = item.itemCtrUV1h
    lb.itemCtrUV2h = item.itemCtrUV2h
    lb.itemCtrUV6h = item.itemCtrUV6h
    lb.itemCtrUV12h = item.itemCtrUV12h
    lb.itemTimes1h = item.itemTimes1h
    lb.itemTimes2h = item.itemTimes2h
    lb.itemTimes6h = item.itemTimes6h
    lb.itemTimes12h = item.itemTimes12h
    lb.itemEffTimes1h = item.itemEffTimes1h
    lb.itemEffTimes2h = item.itemEffTimes2h
    lb.itemEffTimes6h = item.itemEffTimes6h
    lb.itemEffTimes12h = item.itemEffTimes12h
    lb.itemEffUsers1h = item.itemEffUsers1h
    lb.itemEffUsers2h = item.itemEffUsers2h
    lb.itemEffUsers6h = item.itemEffUsers6h
    lb.itemEffUsers12h = item.itemEffUsers12h
    lb.itemView2BottomTimes1h = item.itemView2BottomTimes1h
    lb.itemView2BottomTimes2h = item.itemView2BottomTimes2h
    lb.itemView2BottomTimes6h = item.itemView2BottomTimes6h
    lb.itemView2BottomTimes12h = item.itemView2BottomTimes12h
    lb.itemTotalTime1h = item.itemTotalTime1h
    lb.itemTotalTime2h = item.itemTotalTime2h
    lb.itemTotalTime6h = item.itemTotalTime6h
    lb.itemTotalTime12h = item.itemTotalTime12h
    lb.itemAvsT1h = item.itemAvsT1h
    lb.itemAvsT2h = item.itemAvsT2h
    lb.itemAvsT6h = item.itemAvsT6h
    lb.itemAvsT12h = item.itemAvsT12h
    lb.itemFiR1h = item.itemFiR1h
    lb.itemFiR2h = item.itemFiR2h
    lb.itemFiR6h = item.itemFiR6h
    lb.itemFiR12h = item.itemFiR12h
    lb.itemValidReadR1h = item.itemValidReadR1h
    lb.itemValidReadR2h = item.itemValidReadR2h
    lb.itemValidReadR6h = item.itemValidReadR6h
    lb.itemValidReadR12h = item.itemValidReadR12h
    lb.itemUVValidReadR1h = item.itemUVValidReadR1h
    lb.itemUVValidReadR2h = item.itemUVValidReadR2h
    lb.itemUVValidReadR6h = item.itemUVValidReadR6h
    lb.itemUVValidReadR12h = item.itemUVValidReadR12h
    lb.itemValidRead2R1h = item.itemValidRead2R1h
    lb.itemValidRead2R2h = item.itemValidRead2R2h
    lb.itemValidRead2R6h = item.itemValidRead2R6h
    lb.itemValidRead2R12h = item.itemValidRead2R12h
    lb.itemUVValidRead2R1h = item.itemUVValidRead2R1h
    lb.itemUVValidRead2R2h = item.itemUVValidRead2R2h
    lb.itemUVValidRead2R6h = item.itemUVValidRead2R6h
    lb.itemUVValidRead2R12h = item.itemUVValidRead2R12h
    lb
  }

  def copyItem2LbForDIN(lb: CommonLabelPoint2, item: CommonLabelPoint2) = {
    lb.itemTexL = item.itemTexL
    lb.itemKwN = item.itemKwN
    lb.itemTitL = item.itemTitL
    lb.itemTwN = item.itemTwN
    lb.itemImgN = item.itemImgN
    lb.itemSubT = item.itemSubT
    lb.itemSour = item.itemSour
    lb.itemCreT = item.itemCreT
    lb.itemAuthor = item.itemAuthor
    lb.itemTtP = item.itemTtP
    lb.itemKtW = item.itemKtW
    lb.itemKtW2 = item.itemKtW2
    lb.itemTag1 = item.itemTag1
    lb.itemTag2 = item.itemTag2
    lb.itemTag3 = item.itemTag3
    lb.itemKs1 = item.itemKs1
    lb.itemKs2 = item.itemKs2
    lb.itemCliN_Inc = item.itemCliN_Inc
    lb.itemShoN_Inc = item.itemShoN_Inc
    lb.itemRevi = item.itemRevi
    lb.itemColN = item.itemColN
    lb.itemShare = item.itemShare
    lb.itemVreN = item.itemVreN
    lb.itemLireN = item.itemLireN
    lb.itemRepN = item.itemRepN
    lb.itemEffUsers = item.itemEffUsers
    lb.itemView2BottomTimes = item.itemView2BottomTimes
    lb.itemTotalTime = item.itemTotalTime
    lb
  }
}
