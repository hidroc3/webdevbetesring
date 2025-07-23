import {
  Controller,
  Get,
  Post,
  Body,
  Patch,
  Param,
  Delete,
} from '@nestjs/common';
import { MessageWaService } from './message-wa.service';
import { CreateMessageWaDto } from './dto/create-message-wa.dto';
import { UpdateMessageWaDto } from './dto/update-message-wa.dto';

@Controller('message-wa')
export class MessageWaController {
  constructor(private readonly service: MessageWaService) {}

  @Post()
  async create(@Body() dto: CreateMessageWaDto) {
    const data = await this.service.create(dto);
    return { message: 'Message WA created successfully', data };
  }

  @Get()
  async findAll() {
    const data = await this.service.findAll();
    return { message: 'All Message WA data retrieved', data };
  }

  @Get(':id')
  async findOne(@Param('id') id: string) {
    const data = await this.service.findOne(+id);
    return { message: 'Message WA detail retrieved', data };
  }

  @Patch(':id')
  async update(@Param('id') id: string, @Body() dto: UpdateMessageWaDto) {
    const data = await this.service.update(+id, dto);
    return { message: 'Message WA updated successfully', data };
  }

  @Delete(':id')
  async remove(@Param('id') id: string) {
    const data = await this.service.remove(+id);
    return { message: 'Message WA deleted successfully', data };
  }
}
