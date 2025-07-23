import { Test, TestingModule } from '@nestjs/testing';
import { AwlrLogsController } from './awlr-logs.controller';

describe('AwlrLogsController', () => {
  let controller: AwlrLogsController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [AwlrLogsController],
    }).compile();

    controller = module.get<AwlrLogsController>(AwlrLogsController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
