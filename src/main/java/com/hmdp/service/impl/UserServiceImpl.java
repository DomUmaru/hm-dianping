package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1.校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            //2.不符合，返回错误信息
            return Result.fail("手机格式错误");
        }
        //3.符合，生成验证码
        String code = RandomUtil.randomNumbers(6);
        //4.保存验证码到session
        //session.setAttribute("code", s);
        stringRedisTemplate.opsForValue().set("login:code:" + phone, code, 2, TimeUnit.MINUTES);
        //5.发送验证码
        log.debug("发送短信验证码成功, 验证码:{}", code);
        //6.返回ok
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //1.检验手机号
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            //2.不符合，返回错误信息
            return Result.fail("手机格式错误");
        }
        //2.检验验证码
        //2new.从redis中验证
        //Object cachecode = session.getAttribute("code");
        String cachecode = stringRedisTemplate.opsForValue().get("login:code:" + phone);
        String code = loginForm.getCode();
        if (cachecode == null || !cachecode.toString().equals(code)) {
            //3.不一致，报错
            return Result.fail("验证码错误");
        }
        //4.一致，感觉手机号查询用户
        User user = query().eq("phone", phone).one();

        //5.判断用户是否存在
        if (user == null) {
            //6.不存在，创造用户并保存
            user = createUserWithPhone(phone);
        }

        //7.保存用户信息到session中
        //new7.redis
        //session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));
        //7.1生成随机token
        String token = UUID.randomUUID().toString();
        //7.2将User对象转为Hash存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));
        stringRedisTemplate.opsForHash().putAll("login:token:" + token, userMap);
        //7.3设置有效期
        stringRedisTemplate.expire("login:token:" + token, 30, TimeUnit.MINUTES);
        return Result.ok(token);
    }

    private User createUserWithPhone(String phone) {
        //1.创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName("user_" + RandomUtil.randomNumbers(10));
        //2.保存用户
        save(user);
        return user;
    }
}
